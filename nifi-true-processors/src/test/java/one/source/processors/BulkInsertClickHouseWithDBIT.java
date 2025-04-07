
package one.source.processors;

import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("resource")
public class BulkInsertClickHouseWithDBIT {

    private final long MILLIS_TIMESTAMP_LONG = 1707238288351L;
    private final long MICROS_TIMESTAMP_LONG = 1707238288351567L;
    private final String MICROS_TIMESTAMP_FORMATTED = "2024-02-06 11:51:28.351567";
    private final double MICROS_TIMESTAMP_DOUBLE = ((double) MICROS_TIMESTAMP_LONG) / 1000000D;
    private final long NANOS_AFTER_SECOND = 351567000L;
    private final Instant INSTANT_MICROS_PRECISION = Instant.ofEpochMilli(MILLIS_TIMESTAMP_LONG).plusNanos(NANOS_AFTER_SECOND).minusMillis(MILLIS_TIMESTAMP_LONG % 1000);

    private static final String SIMPLE_INPUT_RECORD = """
            {
              "id": 1,
              "name": "John Doe",
              "age": 50,
              "favorite_color": "blue"
            }
            """;

    private static final String FAVORITE_COLOR_FIELD = "favorite_color";
    private static final String FAVORITE_COLOR = "blue";

    private static ClickHouseContainer clickhouse;
    private TestRunner runner;


    @BeforeAll
    public static void startClickhouse() {
        clickhouse = new ClickHouseContainer("clickhouse/clickhouse-server:25.3")
                .withInitScript("BulkInsertClickhouseIT/create-person-table-witharray.sql");
        clickhouse.start();
    }

    @AfterAll
    public static void cleanup() {
        if (clickhouse != null) {
            clickhouse.close();
            clickhouse = null;
        }
    }

    @BeforeEach
    public void setup() throws InitializationException, SQLException {
        truncateTable();

        runner = TestRunners.newTestRunner(BulkInsertClickhouse.class);
        final DBCPConnectionPool connectionPool = new DBCPConnectionPool();
        runner.addControllerService("connectionPool", connectionPool);
        runner.setProperty(connectionPool, DBCPProperties.DATABASE_URL, clickhouse.getJdbcUrl());
        runner.setProperty(connectionPool, DBCPProperties.DB_USER, clickhouse.getUsername());
        runner.setProperty(connectionPool, DBCPProperties.DB_PASSWORD, clickhouse.getPassword());
        runner.setProperty(connectionPool, DBCPProperties.DB_DRIVERNAME, clickhouse.getDriverClassName());
        runner.enableControllerService(connectionPool);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.setProperty(jsonReader, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd");
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSSSSS");
        runner.enableControllerService(jsonReader);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "json-reader");
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, "connectionPool");

        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "person");
        runner.setProperty(BulkInsertClickhouse.DB_TYPE, "Generic");
//        runner.setProperty(PutDatabaseRecord_copy.STATEMENT_TYPE, "INSERT");
    }


    @Test
    public void testSimplePut() throws SQLException {
        runner.enqueue(SIMPLE_INPUT_RECORD);
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(FAVORITE_COLOR, results.get(FAVORITE_COLOR_FIELD));
    }


    @Test
    public void testWithDate() throws SQLException {
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "dob": "1975-01-01"
            }
            """);
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Date dob = (Date) results.get("dob");
        assertEquals(1975, dob.toLocalDate().getYear());
        assertEquals(Month.JANUARY, dob.toLocalDate().getMonth());
        assertEquals(1, dob.toLocalDate().getDayOfMonth());
    }

    @Test
    public void testWithTimestampUsingMillis() throws SQLException {
        runner.enqueue(createJson(MILLIS_TIMESTAMP_LONG));
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(new Timestamp(MILLIS_TIMESTAMP_LONG), results.get("lasttransactiontime"));
    }

    @Test
    public void testWithTimestampUsingMillisAsString() throws SQLException {
        runner.enqueue(createJson(MILLIS_TIMESTAMP_LONG));
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(new Timestamp(MILLIS_TIMESTAMP_LONG), results.get("lasttransactiontime"));
    }

    @Test
    public void testWithStringTimestampUsingMicros() throws SQLException {
        runner.enqueue(createJson(MICROS_TIMESTAMP_FORMATTED));
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        final LocalDateTime transactionLocalTime = lastTransactionTime.toLocalDateTime();
        assertEquals(2024, transactionLocalTime.getYear());
        assertEquals(Month.FEBRUARY, transactionLocalTime.getMonth());
        assertEquals(6, transactionLocalTime.getDayOfMonth());
        assertEquals(11, transactionLocalTime.getHour());
        assertEquals(51, transactionLocalTime.getMinute());
        assertEquals(28, transactionLocalTime.getSecond());
        assertEquals(351567000, transactionLocalTime.getNano());
    }

    @Test
    public void testWithNumericTimestampUsingMicros() throws SQLException {
        runner.enqueue(createJson(MICROS_TIMESTAMP_LONG));
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(INSTANT_MICROS_PRECISION, lastTransactionTime.toInstant());
    }


    @Test
    public void testWithDecimalTimestampUsingMicros() throws SQLException {
        runner.enqueue(createJson(Double.toString(MICROS_TIMESTAMP_DOUBLE)));
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(INSTANT_MICROS_PRECISION, lastTransactionTime.toInstant());
    }

    @Test
    public void testWithDecimalTimestampUsingMicrosAsString() throws SQLException {
        runner.enqueue(createJson(Double.toString(MICROS_TIMESTAMP_DOUBLE)));
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(INSTANT_MICROS_PRECISION, lastTransactionTime.toInstant());
    }

    @Test
    public void testWithTupleValues() throws SQLException {
//        runner.setProperty(BulkInsertClickhouse.DATA_TYPE_INFERENCE, BulkInsertClickhouse.JSON_DATA_TYPE_INFER.getValue());

        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": "2024-02-06 11:51:28.351567",
              "favorite_color": "blue",
              "remarks": {
                "text": "New York",
                "lastEditedAt": "NY"
              }
            }
            """);
        runner.run();
        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(FAVORITE_COLOR, results.get(FAVORITE_COLOR_FIELD));
    }

    private static void truncateTable() throws SQLException {
        try (final Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword())) {
            final String sqlQuery = "TRUNCATE TABLE person";
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                preparedStatement.execute();
            }
        }
    }

    private Map<String, Object> getResults() throws SQLException {
        try (final Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword())) {
            final String sqlQuery = "SELECT * FROM person";
            final Map<String, Object> resultsMap = new HashMap<>();

            try (final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
                 final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        final String columnName = metaData.getColumnName(i);
                        final Object columnValue = resultSet.getObject(i);
                        resultsMap.put(columnName, columnValue);
                    }
                }
            }

            assertEquals("John Doe", resultsMap.get("name"));
            assertEquals(50, resultsMap.get("age"));

            return resultsMap;
        }
    }

    private String createJson(final long lastTransactionTime) {
        return createJson(Long.toString(lastTransactionTime));
    }

    private String createJson(final String lastTransactionTime) {
        return """
            {
              "id": 1,
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": "%s"
            }""".formatted(lastTransactionTime);
    }
}

 