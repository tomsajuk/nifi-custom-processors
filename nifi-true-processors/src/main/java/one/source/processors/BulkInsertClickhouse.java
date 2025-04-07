package one.source.processors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.standard.db.*;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sql", "record", "clickhouse", "put", "database", "insert"})
@CapabilityDescription("The BulkInsertClickhouse processor uses a specified RecordReader to input (possibly multiple) records from an incoming flow file. These records are translated to SQL "
        + "statements and executed as a single transaction. If any errors occur, the flow file is routed to failure or retry, and if the records are transmitted successfully, "
        + "the incoming flow file is "
        + "routed to success.  This only applied to Clickhouse based Bulk Inserts.")
@WritesAttribute(attribute = BulkInsertClickhouse.PUT_DATABASE_RECORD_ERROR, description = "If an error occurs during processing, the flow file will be routed to failure or retry, and this attribute "
        + "will be populated with the cause of the error.")
@UseCase(description = "Bulk Inserts records into Clickhouse")
public class BulkInsertClickhouse extends AbstractProcessor {

    public static final String INSERT_TYPE = "INSERT";

    static final String PUT_DATABASE_RECORD_ERROR = "bulkinsertclickhouse.error";

    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Any field in the document that cannot be mapped to a column in the database is ignored");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail on Unmatched Fields", "Fail on Unmatched Fields",
            "If the document has any field that cannot be mapped to a column in the database, the FlowFile will be routed to the failure relationship");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  No notification will be logged");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  A warning will be logged");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "A flow will fail if any column in the database that does not have a field in the document.  An error will be logged");


    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RETRY
    );
    // Properties
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("put-db-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("put-db-record-dcbp-service")
            .displayName("Clickhouse Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database for sending records.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the catalog name must match the database's catalog name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the schema name must match the database's schema name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-table-name")
            .displayName("Table Name")
            .description("The name of the table that the statement should affect. Note that if the database is case-sensitive, the table name must match the database's table name exactly.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final AllowableValue BINARY_STRING_FORMAT_UTF8 = new AllowableValue(
            "UTF-8",
            "UTF-8",
            "String values for binary columns contain the original value as text via UTF-8 character encoding"
    );

    static final AllowableValue BINARY_STRING_FORMAT_HEXADECIMAL = new AllowableValue(
            "Hexadecimal",
            "Hexadecimal",
            "String values for binary columns contain the original value in hexadecimal format"
    );

    static final AllowableValue BINARY_STRING_FORMAT_BASE64 = new AllowableValue(
            "Base64",
            "Base64",
            "String values for binary columns contain the original value in Base64 encoded format"
    );

    static final PropertyDescriptor BINARY_STRING_FORMAT = new PropertyDescriptor.Builder()
            .name("put-db-record-binary-format")
            .displayName("Binary String Format")
            .description("The format to be applied when decoding string values to binary.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .allowableValues(BINARY_STRING_FORMAT_UTF8, BINARY_STRING_FORMAT_HEXADECIMAL, BINARY_STRING_FORMAT_BASE64)
            .defaultValue(BINARY_STRING_FORMAT_UTF8.getValue())
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("put-db-record-translate-field-names")
            .displayName("Translate Field Names")
            .description("If true, the Processor will attempt to translate field names into the appropriate column names for the table specified. "
                    + "If false, the field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor TRANSLATION_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("Column Name Translation Strategy")
            .description("The strategy used to normalize table column name. Column Name will be uppercased to " +
                    "do case-insensitive matching irrespective of strategy")
            .allowableValues(TranslationStrategy.class)
            .defaultValue(TranslationStrategy.REMOVE_UNDERSCORE.getValue())
            .dependsOn(TRANSLATE_FIELD_NAMES, TRANSLATE_FIELD_NAMES.getDefaultValue())
            .build();

    public static final PropertyDescriptor TRANSLATION_PATTERN = new PropertyDescriptor.Builder()
            .required(true)
            .name("Column Name Translation Pattern")
            .description("Column name will be normalized with this regular expression")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .dependsOn(TRANSLATE_FIELD_NAMES, TRANSLATE_FIELD_NAMES.getDefaultValue())
            .dependsOn(TRANSLATION_STRATEGY, TranslationStrategy.PATTERN.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("put-db-record-unmatched-field-behavior")
            .displayName("Unmatched Field Behavior")
            .description("If an incoming record has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("put-db-record-unmatched-column-behavior")
            .displayName("Unmatched Column Behavior")
            .description("If an incoming record does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();

    static final PropertyDescriptor QUOTE_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("put-db-record-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTE_TABLE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("put-db-record-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the use of special characters in the table name.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("put-db-record-query-timeout")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL statement "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("table-schema-cache-size")
            .displayName("Table Schema Cache Size")
            .description("Specifies how many Table Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-db-record-max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Specifies maximum number of sql statements to be included in each batch sent to the database. Zero means the batch size is not limited, "
                    + "and all statements are put into a single batch which can cause high memory usage issues for a very large number of statements.")
            .defaultValue("1000")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("database-session-autocommit")
            .displayName("Database Session AutoCommit")
            .description("The autocommit mode to set on the database connection being used. If set to false, the operation(s) will be explicitly committed or rolled back "
                    + "(based on success or failure respectively). If set to true, the driver/database automatically handles the commit/rollback.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(false)
            .build();

    static final PropertyDescriptor DB_TYPE = DatabaseAdapterDescriptor.getDatabaseTypeDescriptor("db-type");
    static final PropertyDescriptor DATABASE_DIALECT_SERVICE = DatabaseAdapterDescriptor.getDatabaseDialectServiceDescriptor(DB_TYPE);

    protected static final List<PropertyDescriptor> properties = List.of(
            RECORD_READER_FACTORY,
            DB_TYPE,
            DATABASE_DIALECT_SERVICE,
            DBCP_SERVICE,
            CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            BINARY_STRING_FORMAT,
            TRANSLATE_FIELD_NAMES,
            TRANSLATION_STRATEGY,
            TRANSLATION_PATTERN,
            UNMATCHED_FIELD_BEHAVIOR,
            UNMATCHED_COLUMN_BEHAVIOR,
            QUOTE_IDENTIFIERS,
            QUOTE_TABLE_IDENTIFIER,
            QUERY_TIMEOUT,
            RollbackOnFailure.ROLLBACK_ON_FAILURE,
            TABLE_SCHEMA_CACHE_SIZE,
            MAX_BATCH_SIZE,
            AUTO_COMMIT
    );

    private Cache<SchemaKey, TableSchema> schemaCache;

    private volatile DatabaseDialectService databaseDialectService;
    private volatile Function<Record, String> recordPathOperationType;
    private volatile RecordPath dataRecordPath;


    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        final Boolean autoCommit = validationContext.getProperty(AUTO_COMMIT).asBoolean();
        final boolean rollbackOnFailure = validationContext.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        if (autoCommit != null && autoCommit && rollbackOnFailure) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName())
                    .explanation(format("'%s' cannot be set to 'true' when '%s' is also set to 'true'. "
                                    + "Transaction rollbacks for batch updates cannot rollback all the flow file's statements together "
                                    + "when auto commit is set to 'true' because the database autocommits each batch separately.",
                            RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName(), AUTO_COMMIT.getDisplayName()))
                    .build());
        }

        if (autoCommit != null && autoCommit && !isMaxBatchSizeHardcodedToZero(validationContext)) {
            final String explanation = format("'%s' must be hard-coded to zero when '%s' is set to 'true'."
                            + " Batch size equal to zero executes all statements in a single transaction"
                            + " which allows rollback to revert all the flow file's statements together if an error occurs.",
                    MAX_BATCH_SIZE.getDisplayName(), AUTO_COMMIT.getDisplayName());

            validationResults.add(new ValidationResult.Builder()
                    .subject(MAX_BATCH_SIZE.getDisplayName())
                    .explanation(explanation)
                    .build());
        }

        return validationResults;
    }

    private boolean isMaxBatchSizeHardcodedToZero(ValidationContext validationContext) {
        try {
            return 0 == validationContext.getProperty(MAX_BATCH_SIZE).asInteger();
        } catch (Exception ex) {
            return false;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String databaseType = context.getProperty(DB_TYPE).getValue();
        databaseDialectService = DatabaseAdapterDescriptor.getDatabaseDialectService(context, DATABASE_DIALECT_SERVICE, databaseType);

        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();

        recordPathOperationType = null;
        dataRecordPath = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int currentQueueSize = session.getQueueSize().getObjectCount();
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        if (currentQueueSize == 0) {
            return;
        }

        int flowFilesToProcess = currentQueueSize;
        if (maxBatchSize > 0) {
            flowFilesToProcess = Math.min(currentQueueSize, maxBatchSize);
        }
        getLogger().debug("Processing {} flow files", flowFilesToProcess);

        List<FlowFile> flowFiles = session.get(flowFilesToProcess);
        if (flowFiles.isEmpty()) {
            return;
        }

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        Connection connection = null;
        boolean originalAutoCommit = false;
        try {
            connection = dbcpService.getConnection(flowFiles.getFirst().getAttributes());

            originalAutoCommit = connection.getAutoCommit();
            final Boolean propertyAutoCommitValue = context.getProperty(AUTO_COMMIT).asBoolean();
            if (propertyAutoCommitValue != null && originalAutoCommit != propertyAutoCommitValue) {
                try {
                    connection.setAutoCommit(propertyAutoCommitValue);
                } catch (Exception ex) {
                    getLogger().debug("Failed to setAutoCommit({}) due to {}", propertyAutoCommitValue, ex.getClass().getName(), ex);
                }
            }

            List<FlowFile> successFlowFiles = putToDatabase(context, session, flowFiles, connection);

            // If the connection's auto-commit setting is false, then manually commit the transaction
            if (!connection.getAutoCommit()) {
                connection.commit();
            }

            for (FlowFile flowFile : successFlowFiles) {
                if (!flowFile.isPenalized()) {
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
                }
            }
        } catch (final Exception e) {
            routeOnException(context, session, connection, e, flowFiles);
        } finally {
            closeConnection(connection, originalAutoCommit);
        }
    }

    private void routeOnException(final ProcessContext context, final ProcessSession session,
                                  Connection connection, Exception e, List<FlowFile> flowFiles) {
        // When an Exception is thrown, we want to route to 'retry' if we expect that attempting the same request again
        // might work. Otherwise, route to failure. SQLTransientException is a specific type that indicates that a retry may work.
        final Relationship relationship;
        final Throwable toAnalyze = (e instanceof BatchUpdateException || e instanceof ProcessException) ? e.getCause() : e;
        if (toAnalyze instanceof SQLTransientException) {
            relationship = REL_RETRY;
            session.penalize(flowFiles.getFirst());
        } else {
            relationship = REL_FAILURE;
        }

        final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        if (rollbackOnFailure) {
            getLogger().error("Failed to put Records to database for {}. Rolling back NiFi session and returning the flow file to its incoming queue.", flowFiles.getFirst(), e);
            session.rollback();
            context.yield();
        } else {
            getLogger().error("Failed to put Records to database for {}. Routing to {}.", flowFiles, relationship, e);
            session.putAttribute(flowFiles.getFirst(), PUT_DATABASE_RECORD_ERROR, (e.getMessage() == null ? "Unknown" : e.getMessage()));
            session.transfer(flowFiles, relationship);
        }

        rollbackConnection(connection);
    }

    private void rollbackConnection(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                    getLogger().debug("Manually rolled back JDBC transaction.");
                }
            } catch (final Exception rollbackException) {
                getLogger().error("Failed to rollback JDBC transaction", rollbackException);
            }
        }
    }

    private void closeConnection(Connection connection, boolean originalAutoCommit) {
        if (connection != null) {
            try {
                if (originalAutoCommit != connection.getAutoCommit()) {
                    connection.setAutoCommit(originalAutoCommit);
                }
            } catch (final Exception autoCommitException) {
                getLogger().warn("Failed to set auto-commit back to {} on connection", originalAutoCommit, autoCommitException);
            }

            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (final Exception closeException) {
                getLogger().warn("Failed to close database connection", closeException);
            }
        }
    }

    private List<FlowFile> executeDML(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles,
                                      final Connection con, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final ComponentLog log = getLogger();
        List<FlowFile> successFlowFiles = new ArrayList<>();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFiles.getFirst()).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFiles.getFirst()).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFiles.getFirst()).getValue();
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final String binaryStringFormat = context.getProperty(BINARY_STRING_FORMAT).evaluateAttributeExpressions(flowFiles.getFirst()).getValue();

        // Ensure the table name has been set, the generated SQL statements (and TableSchema cache) will need it
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(format("Cannot process %s because Table Name is null or empty", flowFiles.getFirst()));
        }
        log.debug("Starts executeDML");
        final NameNormalizer normalizer = Optional.of(settings)
                .filter(s -> s.translateFieldNames)
                .map(s -> NameNormalizerFactory.getNormalizer(s.translationStrategy, s.translationPattern))
                .orElse(null);
        final SchemaKey schemaKey = new BulkInsertClickhouse.SchemaKey(catalog, schemaName, tableName);
        final TableSchema tableSchema;
        try {
            tableSchema = schemaCache.get(schemaKey, key -> {
                try {
                    final TableSchema schema = TableSchema.from(con, catalog, schemaName, tableName, settings.translateFieldNames, normalizer, null, log);
                    getLogger().info("Fetched Table Schema {} for table name {}", schema, tableName);
                    return schema;
                } catch (SQLException e) {
                    // Wrap this in a runtime exception, it is unwrapped in the outer try
                    throw new ProcessException(e);
                }
            });
            if (tableSchema == null) {
                throw new IllegalArgumentException("No table schema specified!");
            }
        } catch (ProcessException pe) {
            // Unwrap the SQLException if one occurred
            if (pe.getCause() instanceof SQLException) {
                throw (SQLException) pe.getCause();
            } else {
                throw pe;
            }
        }

        // build the fully qualified table name
        final String fqTableName = generateTableName(settings, catalog, schemaName, tableName, tableSchema);
        log.debug("Fully qualified table name: {}, maxBatchSize: {}", fqTableName, maxBatchSize);


        final SqlAndIncludedColumns sqlHolder = generateInsertFromTableSchema(fqTableName, tableSchema, settings, normalizer);
        log.debug("Generated SQL: {}", sqlHolder.getSql());
        final PreparedStatement preparedStatement = con.prepareStatement(sqlHolder.getSql());
        try {
            preparedStatement.setQueryTimeout(timeoutMillis); // timeout in seconds
        } catch (final SQLException se) {
            // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
            if (timeoutMillis > 0) {
                throw se;
            }
        }
        final int parameterCount = getParameterCount(sqlHolder.sql);
        final PreparedSqlAndColumns preparedSqlAndColumns = new PreparedSqlAndColumns(sqlHolder, preparedStatement, parameterCount);

        log.debug("PreparedSqlAndColumns: {}", preparedSqlAndColumns);

        int currentBatchSize = 0;
        int batchIndex = 0;
        Record outerRecord;
        PreparedStatement lastPreparedStatement = null;

        try {
            for (FlowFile flowFile : flowFiles) {
                if (flowFile == null) {
                    continue;
                }

                try (final InputStream in = session.read(flowFile)) {
                    final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
                    final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());

                    while ((outerRecord = recordReader.nextRecord()) != null) {
                        final List<Record> dataRecords = getDataRecords(outerRecord);
                        for (final Record currentRecord : dataRecords) {
                            final PreparedStatement ps = preparedSqlAndColumns.getPreparedStatement();
                            final List<String> fieldIndexes = preparedSqlAndColumns.getSqlAndIncludedColumns().getFieldIndexes();

                            lastPreparedStatement = ps;

                            final Object[] values = currentRecord.getValues();
                            final List<DataType> dataTypes = currentRecord.getSchema().getDataTypes();
                            final RecordSchema recordSchema = currentRecord.getSchema();
                            final Map<String, ColumnDescription> columns = tableSchema.getColumns();
                            List<String> addedFieldNames = new ArrayList<>();

                            for (int i = 0; i < values.length; i++) {
                                Object currentValue = values[i];
                                final DataType dataType = dataTypes.get(i);
                                final int fieldSqlType = DataTypeUtils.getSQLTypeValue(dataType);
                                final String fieldName = recordSchema.getField(i).getFieldName();
                                String columnName = TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer);
                                int sqlType;

                                final ColumnDescription column = columns.get(columnName);
                                // 'column' should not be null here as the fieldIndexes should correspond to fields that match table columns, but better to handle just in case
                                if (column == null) {
                                    if (!settings.ignoreUnmappedFields) {
                                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", columns.keySet()));
                                    } else {
                                        sqlType = fieldSqlType;
                                    }
                                } else {
                                    sqlType = column.getDataType();
                                    // SQLServer returns -150 for sql_variant from DatabaseMetaData though the server expects -156 when setting a sql_variant parameter
                                    if (sqlType == -150) {
                                        sqlType = -156;
                                    }
                                }

                                // Convert (if necessary) from field data type to column data type
                                if (fieldSqlType != sqlType) {
                                    try {
                                        DataType targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(sqlType);
                                        // If sqlType is unsupported, fall back to the fieldSqlType instead
                                        if (targetDataType == null) {
                                            targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(fieldSqlType);
                                        }
                                        if (targetDataType != null) {
                                            if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
                                                if (currentValue instanceof Object[] src) {
                                                    // Convert Object[Byte] arrays to byte[]
                                                    if (src.length > 0) {
                                                        if (!(src[0] instanceof Byte)) {
                                                            throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                                        }
                                                    }
                                                    byte[] dest = new byte[src.length];
                                                    for (int j = 0; j < src.length; j++) {
                                                        dest[j] = (Byte) src[j];
                                                    }
                                                    currentValue = dest;
                                                } else if (currentValue instanceof String stringValue) {
                                                    if (BINARY_STRING_FORMAT_BASE64.getValue().equals(binaryStringFormat)) {
                                                        currentValue = Base64.getDecoder().decode(stringValue);
                                                    } else if (BINARY_STRING_FORMAT_HEXADECIMAL.getValue().equals(binaryStringFormat)) {
                                                        currentValue = HexFormat.of().parseHex(stringValue);
                                                    } else {
                                                        currentValue = stringValue.getBytes(StandardCharsets.UTF_8);
                                                    }
                                                } else if (currentValue != null && !(currentValue instanceof byte[])) {
                                                    throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                                }
                                            } else {
                                                currentValue = DataTypeUtils.convertType(
                                                        currentValue,
                                                        targetDataType,
                                                        fieldName);
                                            }
                                        }
                                    } catch (IllegalTypeConversionException itce) {
                                        // If the field and column types don't match or the value can't otherwise be converted to the column datatype,
                                        // try with the original object and field datatype
                                        sqlType = DataTypeUtils.getSQLTypeValue(dataType);
                                    }
                                }

                                final int queryColumnIndex = fieldIndexes.indexOf(columnName);
                                if (queryColumnIndex < 0) {
                                    if (settings.ignoreUnmappedFields) {
                                        log.debug("Field '{}' does not map to any column in the database. Ignoring field.", fieldName);
                                        continue;
                                    } else {
                                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", columns.keySet()));
                                    }
                                }
                                setParameter(ps, queryColumnIndex + 1, currentValue, fieldSqlType, sqlType, columnName);
                                addedFieldNames.add(columnName);
                            }

                            // If the column is not in the list of addedFieldNames, it means that the column is not in the record
                            // and the column is not nullable, so we need to set it to null
                            for (String columnName : columns.keySet()) {
                                if (!addedFieldNames.contains(columnName)) {
                                    ColumnDescription column = columns.get(columnName);
                                    ps.setNull(fieldIndexes.indexOf(columnName) + 1, column.getDataType());
                                }
                            }

                            ps.addBatch();
                            session.adjustCounter(INSERT_TYPE + " updates performed", 1, false);
                        }
                    }
                    successFlowFiles.add(flowFile);
                    currentBatchSize++;
                } catch (final Exception e) {
                    flowFile = session.penalize(flowFile);
                    log.error("Failed to Process flowfile for {} due to {}", new Object[]{flowFile, e.getMessage()}, e);
                    flowFile = session.putAttribute(flowFile, PUT_DATABASE_RECORD_ERROR, (e.getMessage() == null ? "Unknown" : e.getMessage()));
                    session.transfer(flowFile, REL_FAILURE);
                }
            }

            if (currentBatchSize > 0) {
                lastPreparedStatement.executeBatch();
                getLogger().info("Executing with batch size for {}; fieldIndexes: {}; batch index: {}",
                        currentBatchSize, preparedSqlAndColumns.getSqlAndIncludedColumns().getFieldIndexes(), batchIndex);
                session.adjustCounter("Batches Executed", 1, false);
            }
        } finally {
            preparedSqlAndColumns.getPreparedStatement().close();
        }
        getLogger().debug("In the final line of executeDML");
        return successFlowFiles;
    }

    private void setParameter(PreparedStatement ps, int index, Object value, int fieldSqlType, int sqlType, String columnName) throws IOException {
        if (sqlType == Types.BLOB) {
            // Convert Byte[] or String (anything that has been converted to byte[]) into BLOB
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement column" + columnName, e);
                        }
                    } else {
                        throw new IOException("Expected BLOB to be of type byte[] but is instead " + value.getClass().getName() + " for column " + columnName);
                    }
                }
                byte[] byteArray = (byte[]) value;
                try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
                    ps.setBlob(index, inputStream);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data " + value + " for column " + columnName, e);
                }
            } else {
                try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8))) {
                    ps.setBlob(index, inputStream);
                } catch (IOException | SQLException e) {
                    throw new IOException("Unable to parse binary data " + value + " for column " + columnName, e);
                }
            }
        } else if (sqlType == Types.CLOB) {
            if (value == null) {
                try {
                    ps.setNull(index, Types.CLOB);
                } catch (SQLException e) {
                    throw new IOException("Unable to setNull() on prepared statement  for column " + columnName, e);
                }
            } else {
                try {
                    Clob clob = ps.getConnection().createClob();
                    clob.setString(1, value.toString());
                    ps.setClob(index, clob);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse data as CLOB/String " + value + " for column " + columnName, e);
                }
            }
        } else if (sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement for column " + columnName, e);
                        }
                    } else {
                        throw new IOException("Expected VARBINARY/LONGVARBINARY to be of type byte[] but is instead " + value.getClass().getName() + " for column " + columnName);
                    }
                }
                byte[] byteArray = (byte[]) value;
                try {
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length + " for column " + columnName, e);
                }
            } else {
                byte[] byteArray = new byte[0];
                try {
                    byteArray = value.toString().getBytes(StandardCharsets.UTF_8);
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length + " for column " + columnName, e);
                }
            }
        } else {
            try {
                // If the specified field type is OTHER and the SQL type is VARCHAR, the conversion went ok as a string literal but try the OTHER type when setting the parameter. If an error occurs,
                // try the normal way of using the sqlType
                // This helps with PostgreSQL enums and possibly other scenarios
                if (fieldSqlType == Types.OTHER && sqlType == Types.VARCHAR) {
                    try {
                        ps.setObject(index, value, fieldSqlType);
                    } catch (SQLException e) {
                        // Fall back to default setObject params
                        ps.setObject(index, value, sqlType);
                    }
                } else {
                    ps.setObject(index, value, sqlType);
                }
            } catch (SQLException e) {
                throw new IOException("Unable to setObject() with value " + value + " at index " + index + " of type " + sqlType + " for column " + columnName, e);
            }
        }
    }

    private List<Record> getDataRecords(final Record outerRecord) {
        if (dataRecordPath == null) {
            getLogger().info("outer record {}", outerRecord);
            return List.of(outerRecord);
        }

        final RecordPathResult result = dataRecordPath.evaluate(outerRecord);
        final List<FieldValue> fieldValues = result.getSelectedFields().toList();
        if (fieldValues.isEmpty()) {
            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
        }

        for (final FieldValue fieldValue : fieldValues) {
            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
            if (fieldType != RecordFieldType.RECORD) {
                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                        " " + fieldType);
            }
        }

        final List<Record> dataRecords = new ArrayList<>(fieldValues.size());
        for (final FieldValue fieldValue : fieldValues) {
            dataRecords.add((Record) fieldValue.getValue());
        }

        return dataRecords;
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }

    private List<FlowFile> putToDatabase(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles, final Connection connection) throws Exception {
        final DMLSettings settings = new DMLSettings(context);
        return executeDML(context, session, flowFiles, connection, settings);
    }

    String generateTableName(final DMLSettings settings, final String catalog, final String schemaName, final String tableName, final TableSchema tableSchema) {
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(catalog)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(catalog);
            }

            tableNameBuilder.append(".");
        }

        if (schemaName != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(schemaName)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(schemaName);
            }

            tableNameBuilder.append(".");
        }

        if (settings.quoteTableName) {
            tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            tableNameBuilder.append(tableName);
        }

        return tableNameBuilder.toString();
    }

    private Set<String> getNormalizedColumnNames(final RecordSchema schema, final boolean translateFieldNames, NameNormalizer normalizer) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        if (schema != null) {
            schema.getFieldNames().forEach((fieldName) -> normalizedFieldNames.add(TableSchema.normalizedName(fieldName, translateFieldNames, normalizer)));
        }
        return normalizedFieldNames;
    }

    SqlAndIncludedColumns generateInsertFromTableSchema(final String tableName, final TableSchema tableSchema, final DMLSettings settings, NameNormalizer normalizer)
            throws IllegalArgumentException, SQLException {

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(tableName);
        sqlBuilder.append(" (");

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = tableSchema.getColumns().keySet().stream().toList();
        int fieldCount = fieldNames.size();
        for (int i = 0; i < fieldCount; i++) {
            String fieldName = fieldNames.get(i);
            final ColumnDescription desc = tableSchema.getColumns().get(TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer));
            if (desc == null && !settings.ignoreUnmappedFields) {
                throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }

            if (desc != null) {
                if (i > 0) {
                    sqlBuilder.append(", ");
                }

                if (settings.escapeColumnNames) {
                    sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                            .append(desc.getColumnName())
                            .append(tableSchema.getQuotedIdentifierString());
                } else {
                    sqlBuilder.append(desc.getColumnName());
                }
            } else {
                // User is ignoring unmapped fields, but log at debug level just in case
                getLogger().debug("Did not map field '{}' to any column in the database\n{}Columns: {}",
                        fieldName, (settings.translateFieldNames ? "Normalized " : ""), String.join(",", tableSchema.getColumns().keySet()));
            }
        }


        // complete the SQL statements by adding ?'s for all of the values to be escaped.
        sqlBuilder.append(") VALUES (");
        sqlBuilder.append(StringUtils.repeat("?", ",", fieldNames.size()));
        sqlBuilder.append(")");

        return new SqlAndIncludedColumns(sqlBuilder.toString(), fieldNames);
    }

    private void checkValuesForRequiredColumns(RecordSchema recordSchema, TableSchema tableSchema, DMLSettings settings, NameNormalizer normalizer) {
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames, normalizer);

        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = TableSchema.normalizedName(requiredColName, settings.translateFieldNames, normalizer);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new IllegalArgumentException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }
    }

    private int getParameterCount(final String sql) {
        int parameterCount = 0;
        for (char character : sql.toCharArray()) {
            if ('?' == character) {
                parameterCount++;
            }
        }
        return parameterCount;
    }

    static class SchemaKey {
        private final String catalog;
        private final String schemaName;
        private final String tableName;

        public SchemaKey(final String catalog, final String schemaName, final String tableName) {
            this.catalog = catalog;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int result = catalog != null ? catalog.hashCode() : 0;
            result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
            result = 31 * result + tableName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaKey schemaKey = (SchemaKey) o;

            if (!Objects.equals(catalog, schemaKey.catalog)) return false;
            if (!Objects.equals(schemaName, schemaKey.schemaName))
                return false;
            return tableName.equals(schemaKey.tableName);
        }
    }

    /**
     * A holder class for a SQL prepared statement and a BitSet indicating which columns are being updated (to determine which values from the record to set on the statement)
     * A value of null for getIncludedColumns indicates that all columns/fields should be included.
     */
    static class SqlAndIncludedColumns {
        private final String sql;
        private final List<String> fieldIndexes;

        /**
         * Constructor
         *
         * @param sql          The prepared SQL statement (including parameters notated by ? )
         * @param fieldIndexes A List of record indexes. The index of the list is the location of the record field in the SQL prepared statement
         */
        public SqlAndIncludedColumns(final String sql, final List<String> fieldIndexes) {
            this.sql = sql;
            this.fieldIndexes = fieldIndexes;
        }

        public String getSql() {
            return sql;
        }

        public List<String> getFieldIndexes() {
            return fieldIndexes;
        }
    }

    static class PreparedSqlAndColumns {
        private final SqlAndIncludedColumns sqlAndIncludedColumns;
        private final PreparedStatement preparedStatement;
        private final int parameterCount;

        public PreparedSqlAndColumns(final SqlAndIncludedColumns sqlAndIncludedColumns, final PreparedStatement preparedStatement, final int parameterCount) {
            this.sqlAndIncludedColumns = sqlAndIncludedColumns;
            this.preparedStatement = preparedStatement;
            this.parameterCount = parameterCount;
        }

        public SqlAndIncludedColumns getSqlAndIncludedColumns() {
            return sqlAndIncludedColumns;
        }

        public PreparedStatement getPreparedStatement() {
            return preparedStatement;
        }
    }

    static class DMLSettings {
        private final boolean translateFieldNames;
        private final TranslationStrategy translationStrategy;
        private final Pattern translationPattern;
        private final boolean ignoreUnmappedFields;

        // Is the unmatched column behaviour fail or warning?
        private final boolean failUnmappedColumns;
        private final boolean warningUnmappedColumns;

        // Escape column names?
        private final boolean escapeColumnNames;

        // Quote table name?
        private final boolean quoteTableName;

        DMLSettings(ProcessContext context) {
            translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            translationStrategy = TranslationStrategy.valueOf(context.getProperty(TRANSLATION_STRATEGY).getValue());
            final String translationRegex = context.getProperty(TRANSLATION_PATTERN).getValue();
            translationPattern = translationRegex == null ? null : Pattern.compile(translationRegex);
            ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());

            failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
            warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            escapeColumnNames = context.getProperty(QUOTE_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTE_TABLE_IDENTIFIER).asBoolean();
        }
    }

}


