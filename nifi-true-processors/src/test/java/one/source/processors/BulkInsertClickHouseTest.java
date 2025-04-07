package one.source.processors;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.NameNormalizer;
import org.apache.nifi.processors.standard.db.TableSchema;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordFailureType;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

public class BulkInsertClickHouseTest {

    private enum TestCaseEnum {
        // ENABLED means to use that test case in the parameterized tests.
        // DISABLED test cases are used for single-run tests which are not parameterized
        DEFAULT_0(ENABLED, new TestCase(false, false, 0)),
        DEFAULT_1(DISABLED, new TestCase(false, false, 1)),
        DEFAULT_2(DISABLED, new TestCase(null, false, 2)),
        DEFAULT_5(DISABLED, new TestCase(null, false, 5)),
        DEFAULT_1000(DISABLED, new TestCase(false, false, 1000)),

        ROLLBACK_0(DISABLED, new TestCase(false, true, 0)),
        ROLLBACK_1(ENABLED, new TestCase(null, true, 1)),
        ROLLBACK_2(DISABLED, new TestCase(false, true, 2)),
        ROLLBACK_1000(ENABLED, new TestCase(false, true, 1000)),

        // If autoCommit equals true, then rollbackOnFailure must be false AND batchSize must equal 0
        AUTO_COMMIT_0(ENABLED, new TestCase(true, false, 0));

        private final boolean enabled;
        private final TestCase testCase;

        TestCaseEnum(boolean enabled, TestCase t) {
            this.enabled = enabled;
            this.testCase = t;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public TestCase getTestCase() {
            return testCase;
        }

    }

    static Stream<Arguments> getTestCases() {
        return Arrays.stream(TestCaseEnum.values())
                .filter(TestCaseEnum::isEnabled)
                .map(TestCaseEnum::getTestCase)
                .map(Arguments::of);
    }

    private final static boolean ENABLED = true;
    private final static boolean DISABLED = false;

    private final static String DBCP_SERVICE_ID = "dbcp";

    private static final String CONNECTION_FAILED = "Connection Failed";

    private static final String PARSER_ID = MockRecordParser.class.getSimpleName();

    private static final String TABLE_NAME = "PERSONS";

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private static final String createPersonsSchema1 = "CREATE TABLE SCHEMA1.PERSONS (id integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";
    private static final String createPersonsSchema2 = "CREATE TABLE SCHEMA2.PERSONS (id2 integer primary key, name varchar(100)," +
            " code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000), dt date)";

    private static final String createUUIDSchema = "CREATE TABLE UUID_TEST (id integer primary key, name VARCHAR(100))";

    private static final String createLongVarBinarySchema = "CREATE TABLE LONGVARBINARY_TEST (id integer primary key, name LONG VARCHAR FOR BIT DATA)";

    private final static String DB_LOCATION = "target/db_pdr";

    TestRunner runner;
    BulkInsertClickhouse processor;
    DBCPService dbcp;

    @BeforeAll
    public static void setDatabaseLocation() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignored) {
            // Do nothing, may not have existed
        }
    }

    @AfterAll
    public static void shutdownDatabase() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (Exception ignored) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ignored) {
            // Do nothing, may not have existed
        }
        System.clearProperty("derby.stream.error.file");
    }

    private void setRunner(TestCase testCase) throws InitializationException {
        processor = new BulkInsertClickhouse();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, DBCP_SERVICE_ID);
        if (testCase.getAutoCommitAsString() == null) {
            runner.removeProperty(BulkInsertClickhouse.AUTO_COMMIT);
        } else {
            runner.setProperty(BulkInsertClickhouse.AUTO_COMMIT, testCase.getAutoCommitAsString());
        }
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, testCase.getRollbackOnFailureAsString());
        runner.setProperty(BulkInsertClickhouse.MAX_BATCH_SIZE, testCase.getBatchSizeAsString());
    }

    @Test
    public void testGetConnectionFailure() throws InitializationException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService(PARSER_ID, parser);
        runner.enableControllerService(parser);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, PARSER_ID);
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, TABLE_NAME);

        when(dbcp.getConnection(anyMap())).thenThrow(new ProcessException(CONNECTION_FAILED));

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_FAILURE);
    }

    @Test
    public void testSetAutoCommitFalseFailure() throws InitializationException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

        dbcp = new DBCPServiceAutoCommitTest(DB_LOCATION);
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, DBCP_SERVICE_ID);

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ

        parser.addRecord(1, "rec1", 101, jdbcDate1);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_SUCCESS);
    }

    @Test
    public void testProcessExceptionRouteRetry() throws InitializationException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

        // This exception should route to REL_RETRY because its cause is SQLTransientException
        dbcp = new DBCPServiceThrowConnectionException(new SQLTransientException("connection failed"));
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, DBCP_SERVICE_ID);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_RETRY);
    }

    @Test
    public void testProcessExceptionRouteFailure() throws InitializationException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1.getTestCase());

        // This exception should route to REL_FAILURE because its cause is NOT SQLTransientException
        dbcp = new DBCPServiceThrowConnectionException(new NullPointerException("connection is null"));
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, DBCP_SERVICE_ID);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_FAILURE);
    }

    public void testInsertNonRequiredColumnsUnmatchedField() throws InitializationException, ProcessException {
        setRunner(TestCaseEnum.DEFAULT_5.getTestCase());

        // Need to override the @Before method with a new processor that behaves badly
        processor = new PutDatabaseRecordUnmatchedField();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION));

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, DBCP_SERVICE_ID);

        recreateTable();
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService(PARSER_ID, parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("extra", RecordFieldType.STRING);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date nifiDate1 = new Date(testDate1.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date nifiDate2 = new Date(testDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

        parser.addRecord(1, "rec1", "test", nifiDate1);
        parser.addRecord(2, "rec2", "test", nifiDate2);
        parser.addRecord(3, "rec3", "test", null);
        parser.addRecord(4, "rec4", "test", null);
        parser.addRecord(5, null, null, null);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, PARSER_ID);
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, TABLE_NAME);
        runner.setProperty(BulkInsertClickhouse.UNMATCHED_FIELD_BEHAVIOR, BulkInsertClickhouse.FAIL_UNMATCHED_FIELD);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_RETRY, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_FAILURE, 1);
    }

    @ParameterizedTest()
    @MethodSource("getTestCases")
    public void testInsert(TestCase testCase) throws InitializationException, ProcessException, SQLException {
        setRunner(testCase);

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ

        parser.addRecord(1, "rec1", 101, jdbcDate1);
        parser.addRecord(2, "rec2", 102, jdbcDate2);
        parser.addRecord(3, "rec3", 103, null);
        parser.addRecord(4, "rec4", 104, null);
        parser.addRecord(5, null, 105, null);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertEquals(jdbcDate1.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertEquals(jdbcDate2.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(103, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("rec4", rs.getString(2));
        assertEquals(104, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertNull(rs.getString(2));
        assertEquals(105, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertNonRequiredColumns() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.ROLLBACK_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("dt", RecordFieldType.DATE);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ

        parser.addRecord(1, "rec1", jdbcDate1);
        parser.addRecord(2, "rec2", jdbcDate2);
        parser.addRecord(3, "rec3", null);
        parser.addRecord(4, "rec4", null);
        parser.addRecord(5, null, null);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        // Zero value because of the constraint
        assertEquals(0, rs.getInt(3));
        assertEquals(jdbcDate1.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertEquals(jdbcDate2.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("rec4", rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertNull(rs.getString(2));
        assertEquals(0, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertBatchUpdateException() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        // This record violates the constraint on the "code" column so should result in FlowFile routing to failure
        parser.addRecord(3, "rec3", 1000);
        parser.addRecord(4, "rec4", 104);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertBatchUpdateExceptionRollbackOnFailure() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.ROLLBACK_1000.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 1000);
        parser.addRecord(4, "rec4", 104);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        // Transaction should be rolled back and table should remain empty.
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertNoTableSpecified() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "${not.a.real.attr}");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_FAILURE, 1);
    }

    @Test
    public void testInsertNoTableExists() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.AUTO_COMMIT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS2");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(BulkInsertClickhouse.REL_FAILURE).get(0);
        final String errorMessage = flowFile.getAttribute("bulkinsertclickhouse.error");
        assertTrue(errorMessage.contains("PERSONS2"));
        runner.enqueue();
    }


    @Test
    public void testInvalidData() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 104);

        parser.failAfter(1);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        try {
            // Transaction should be rolled back and table should remain empty.
            assertFalse(rs.next());
        } finally {
            stmt.close();
            conn.close();
        }
    }

    @Test
    public void testIOExceptionOnReadData() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 104);

        parser.failAfter(1, MockRecordFailureType.IO_EXCEPTION);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        try {
            // Transaction should be rolled back and table should remain empty.
            assertFalse(rs.next());
        } finally {
            stmt.close();
            conn.close();
        }
    }

    @Test
    public void testIOExceptionOnReadDataAutoCommit() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.AUTO_COMMIT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        parser.addRecord(1, "rec1", 101);
        parser.addRecord(2, "rec2", 102);
        parser.addRecord(3, "rec3", 104);

        parser.failAfter(1, MockRecordFailureType.IO_EXCEPTION);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(BulkInsertClickhouse.REL_FAILURE, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        try {
            // Transaction should be rolled back and table should remain empty.
            assertFalse(rs.next());
        } finally {
            stmt.close();
            conn.close();
        }
    }

    @Test
    public void testInsertWithMaxBatchSize() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        for (int i = 1; i < 12; i++) {
            parser.addRecord(i, String.format("rec%s", i), 100 + i);
        }

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");
        runner.setProperty(BulkInsertClickhouse.MAX_BATCH_SIZE, "5");

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);

        assertEquals(11, getTableSize());

        assertNotNull(spyStmt.get());
        verify(spyStmt.get(), times(1)).executeBatch();
    }

    @Test
    public void testInsertWithDefaultMaxBatchSize() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_1000.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);

        for (int i = 1; i < 12; i++) {
            parser.addRecord(i, String.format("rec%s", i), 100 + i);
        }

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");
        runner.setProperty(BulkInsertClickhouse.MAX_BATCH_SIZE, BulkInsertClickhouse.MAX_BATCH_SIZE.getDefaultValue());

        Supplier<PreparedStatement> spyStmt = createPreparedStatementSpy();

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);

        assertEquals(11, getTableSize());

        assertNotNull(spyStmt.get());
        verify(spyStmt.get(), times(1)).executeBatch();
    }

    @Test
    public void testGenerateTableName() throws InitializationException, ProcessException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        final TableSchema tableSchema = new TableSchema(
                null,
                null,
                "PERSONS",
                Arrays.asList(
                        new ColumnDescription("id", 4, true, 2, false),
                        new ColumnDescription("name", 12, true, 255, true),
                        new ColumnDescription("code", 4, true, 10, true)
                ),
                false, null,
                new HashSet<>(List.of("id")),
                ""
        );

        runner.setProperty(BulkInsertClickhouse.TRANSLATE_FIELD_NAMES, "false");
        runner.setProperty(BulkInsertClickhouse.UNMATCHED_FIELD_BEHAVIOR, BulkInsertClickhouse.IGNORE_UNMATCHED_FIELD);
        runner.setProperty(BulkInsertClickhouse.UNMATCHED_COLUMN_BEHAVIOR, BulkInsertClickhouse.IGNORE_UNMATCHED_COLUMN);
        runner.setProperty(BulkInsertClickhouse.QUOTE_IDENTIFIERS, "true");
        runner.setProperty(BulkInsertClickhouse.QUOTE_TABLE_IDENTIFIER, "true");
        final BulkInsertClickhouse.DMLSettings settings = new BulkInsertClickhouse.DMLSettings(runner.getProcessContext());

        assertEquals("test_catalog.test_schema.test_table", processor.generateTableName(settings, "test_catalog", "test_schema", "test_table", tableSchema));
    }

    @Test
    public void testInsertMismatchedCompatibleDataTypes() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.BIGINT);

        LocalDate testDate1 = LocalDate.of(2021, 1, 26);
        Date jdbcDate1 = Date.valueOf(testDate1); // in local TZ
        BigInteger nifiDate1 = BigInteger.valueOf(jdbcDate1.getTime()); // in local TZ

        LocalDate testDate2 = LocalDate.of(2021, 7, 26);
        Date jdbcDate2 = Date.valueOf(testDate2); // in local TZ
        BigInteger nifiDate2 = BigInteger.valueOf(jdbcDate2.getTime()); // in local TZ

        parser.addRecord(1, "rec1", 101, nifiDate1);
        parser.addRecord(2, "rec2", 102, nifiDate2);
        parser.addRecord(3, "rec3", 103, null);
        parser.addRecord(4, "rec4", 104, null);
        parser.addRecord(5, null, 105, null);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertEquals(101, rs.getInt(3));
        assertEquals(jdbcDate1.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertEquals(102, rs.getInt(3));
        assertEquals(jdbcDate2.toString(), rs.getDate(4).toString());
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals("rec3", rs.getString(2));
        assertEquals(103, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals("rec4", rs.getString(2));
        assertEquals(104, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertNull(rs.getString(2));
        assertEquals(105, rs.getInt(3));
        assertNull(rs.getDate(4));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertMismatchedNotCompatibleDataTypes() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        recreateTable(createPersons);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.STRING);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("dt", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.FLOAT.getDataType()).getFieldType());

        parser.addRecord("1", "rec1", 101, Arrays.asList(1.0, 2.0));
        parser.addRecord("2", "rec2", 102, Arrays.asList(3.0, 4.0));
        parser.addRecord("3", "rec3", 103, null);
        parser.addRecord("4", "rec4", 104, null);
        parser.addRecord("5", null, 105, null);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        // A SQLFeatureNotSupportedException exception is expected from Derby when you try to put the data as an ARRAY
        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_FAILURE, 1);
    }

    @Test
    public void testLongVarchar() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE TEMP");
        } catch (final Exception ignored) {
            // Do nothing, table may not exist
        }
        stmt.execute("CREATE TABLE TEMP (id integer primary key, name long varchar)");

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);

        parser.addRecord(1, "rec1");
        parser.addRecord(2, "rec2");

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "TEMP");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM TEMP");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("rec1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("rec2", rs.getString(2));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithDifferentColumnOrdering() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE TEMP");
        } catch (final Exception ignored) {
            // Do nothing, table may not exist
        }
        stmt.execute("CREATE TABLE TEMP (id integer primary key, code integer, name long varchar)");

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("code", RecordFieldType.INT);

        // change order of columns
        parser.addRecord("rec1", 1, 101);
        parser.addRecord("rec2", 2, 102);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "TEMP");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM TEMP");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals("rec1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(102, rs.getInt(2));
        assertEquals("rec2", rs.getString(3));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithBlobClob() throws InitializationException, ProcessException, SQLException, IOException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        byte[] bytes = "BLOB".getBytes();
        Byte[] blobRecordValue = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            blobRecordValue[i] = bytes[i];
        }

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.ARRAY);

        parser.addRecord(1, "rec1", 101, blobRecordValue);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Clob clob = rs.getClob(2);
        assertNotNull(clob);
        char[] clobText = new char[5];
        int numBytes = clob.getCharacterStream().read(clobText);
        assertEquals(4, numBytes);
        // Ignore last character, it"s meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals("rec1", new String(clobText).substring(0, 4));
        Blob blob = rs.getBlob(3);
        assertEquals("BLOB", new String(blob.getBytes(1, (int) blob.length())));
        assertEquals(101, rs.getInt(4));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertHexStringIntoBinary() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        runner.setProperty(BulkInsertClickhouse.BINARY_STRING_FORMAT, BulkInsertClickhouse.BINARY_STRING_FORMAT_HEXADECIMAL);

        String tableName = "HEX_STRING_TEST";
        String createTable = "CREATE TABLE " + tableName + " (id integer primary key, binary_data blob)";
        String hexStringData = "abCDef";

        recreateTable(tableName, createTable);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("binaryData", RecordFieldType.STRING);

        parser.addRecord(1, hexStringData);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, tableName);

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();

        final ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
        assertTrue(resultSet.next());

        assertEquals(1, resultSet.getInt(1));

        Blob blob = resultSet.getBlob(2);
        assertArrayEquals(new byte[]{(byte) 171, (byte) 205, (byte) 239}, blob.getBytes(1, (int) blob.length()));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertBase64StringIntoBinary() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        runner.setProperty(BulkInsertClickhouse.BINARY_STRING_FORMAT, BulkInsertClickhouse.BINARY_STRING_FORMAT_BASE64);

        String tableName = "BASE64_STRING_TEST";
        String createTable = "CREATE TABLE " + tableName + " (id integer primary key, binary_data blob)";
        byte[] binaryData = {(byte) 10, (byte) 103, (byte) 234};

        recreateTable(tableName, createTable);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("binaryData", RecordFieldType.STRING);

        parser.addRecord(1, Base64.getEncoder().encodeToString(binaryData));

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, tableName);

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();

        final ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
        assertTrue(resultSet.next());

        assertEquals(1, resultSet.getInt(1));

        Blob blob = resultSet.getBlob(2);
        assertArrayEquals(binaryData, blob.getBytes(1, (int) blob.length()));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithBlobClobObjectArraySource() throws InitializationException, ProcessException, SQLException, IOException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        byte[] bytes = "BLOB".getBytes();
        Object[] blobRecordValue = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            blobRecordValue[i] = bytes[i];
        }

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.ARRAY);

        parser.addRecord(1, "rec1", 101, blobRecordValue);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Clob clob = rs.getClob(2);
        assertNotNull(clob);
        char[] clobText = new char[5];
        int numBytes = clob.getCharacterStream().read(clobText);
        assertEquals(4, numBytes);
        // Ignore last character, it"s meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals("rec1", new String(clobText).substring(0, 4));
        Blob blob = rs.getBlob(3);
        assertEquals("BLOB", new String(blob.getBytes(1, (int) blob.length())));
        assertEquals(101, rs.getInt(4));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithBlobStringSource() throws InitializationException, ProcessException, SQLException, IOException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.STRING);

        parser.addRecord(1, "rec1", 101, "BLOB");

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM PERSONS");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Clob clob = rs.getClob(2);
        assertNotNull(clob);
        char[] clobText = new char[5];
        int numBytes = clob.getCharacterStream().read(clobText);
        assertEquals(4, numBytes);
        // Ignore last character, it"s meant to ensure that only 4 bytes were read even though the buffer is 5 bytes
        assertEquals("rec1", new String(clobText).substring(0, 4));
        Blob blob = rs.getBlob(3);
        assertEquals("BLOB", new String(blob.getBytes(1, (int) blob.length())));
        assertEquals(101, rs.getInt(4));

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertWithBlobIntegerArraySource() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        String createTableWithBlob = "CREATE TABLE PERSONS (id integer primary key, name clob," +
                "content blob, code integer CONSTRAINT CODE_RANGE CHECK (code >= 0 AND code < 1000))";

        recreateTable(createTableWithBlob);
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("content", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()).getFieldType());

        parser.addRecord(1, "rec1", 101, new Integer[]{1, 2, 3});

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "PERSONS");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_RETRY, 0);
        runner.assertTransferCount(BulkInsertClickhouse.REL_FAILURE, 1);
    }

    @Test
    public void testInsertEnum() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION, false)); // Use H2
        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService(DBCP_SERVICE_ID, dbcp, new HashMap<>());
        runner.enableControllerService(dbcp);
        runner.setProperty(BulkInsertClickhouse.DBCP_SERVICE, DBCP_SERVICE_ID);
        try (Connection conn = dbcp.getConnection()) {
            conn.createStatement().executeUpdate("DROP TABLE IF EXISTS ENUM_TEST");
        }
        recreateTable("CREATE TABLE IF NOT EXISTS ENUM_TEST (id integer primary key, suit ENUM('clubs', 'diamonds', 'hearts', 'spades'))");
        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("suit", RecordFieldType.ENUM.getEnumDataType(Arrays.asList("clubs", "diamonds", "hearts", "spades")).getFieldType());

        parser.addRecord(1, "diamonds");
        parser.addRecord(2, "hearts");


        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "ENUM_TEST");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        final ResultSet rs = stmt.executeQuery("SELECT * FROM ENUM_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("diamonds", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("hearts", rs.getString(2));
        assertFalse(rs.next());

        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertUUIDColumn() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        stmt.execute(createUUIDSchema);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.UUID);

        parser.addRecord(1, "425085a0-03ef-11ee-be56-0242ac120002");
        parser.addRecord(2, "56a000e4-03ef-11ee-be56-0242ac120002");

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "UUID_TEST");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM UUID_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("425085a0-03ef-11ee-be56-0242ac120002", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("56a000e4-03ef-11ee-be56-0242ac120002", rs.getString(2));
        assertFalse(rs.next());

        // Drop the schemas here so as not to interfere with other tests
        stmt.execute("drop table UUID_TEST");
        stmt.close();
        conn.close();
    }

    @Test
    public void testInsertLongVarBinaryColumn() throws InitializationException, ProcessException, SQLException {
        setRunner(TestCaseEnum.DEFAULT_0.getTestCase());

        // Manually create and drop the tables and schemas
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        stmt.execute(createLongVarBinarySchema);

        final MockRecordParser parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()).getFieldType());

        byte[] longVarBinaryValue1 = new byte[]{97, 98, 99};
        byte[] longVarBinaryValue2 = new byte[]{100, 101, 102};
        parser.addRecord(1, longVarBinaryValue1);
        parser.addRecord(2, longVarBinaryValue2);

        runner.setProperty(BulkInsertClickhouse.RECORD_READER_FACTORY, "parser");
        runner.setProperty(BulkInsertClickhouse.TABLE_NAME, "LONGVARBINARY_TEST");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(BulkInsertClickhouse.REL_SUCCESS, 1);
        ResultSet rs = stmt.executeQuery("SELECT * FROM LONGVARBINARY_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertArrayEquals(longVarBinaryValue1, rs.getBytes(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertArrayEquals(longVarBinaryValue2, rs.getBytes(2));
        assertFalse(rs.next());

        // Drop the schemas here so as not to interfere with other tests
        stmt.execute("drop table LONGVARBINARY_TEST");
        stmt.close();
        conn.close();
    }

    private void recreateTable() throws ProcessException {
        try (final Connection conn = dbcp.getConnection();
             final Statement stmt = conn.createStatement()) {
            stmt.execute("drop table PERSONS");
            stmt.execute(createPersons);
        } catch (SQLException ignored) {
            // Do nothing, may not have existed
        }
    }

    private int getTableSize() throws SQLException {
        try (final Connection connection = dbcp.getConnection()) {
            try (final Statement stmt = connection.createStatement()) {
                final ResultSet rs = stmt.executeQuery("SELECT count(*) FROM PERSONS");
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }

    private void recreateTable(String createSQL) throws ProcessException, SQLException {
        recreateTable("PERSONS", createSQL);
    }

    private void recreateTable(String tableName, String createSQL) throws ProcessException, SQLException {
        final Connection conn = dbcp.getConnection();
        final Statement stmt = conn.createStatement();
        try {
            stmt.execute("drop table " + tableName);
        } catch (SQLException ignored) {
            // Do nothing, may not have existed
        }
        try (conn; stmt) {
            stmt.execute(createSQL);
        }
    }

    private Map<String, Object> createValues(final int id, final String name, final int code) {
        final Map<String, Object> values = new HashMap<>();
        values.put("id", id);
        values.put("name", name);
        values.put("code", code);
        return values;
    }

    private Supplier<PreparedStatement> createPreparedStatementSpy() {
        final PreparedStatement[] spyStmt = new PreparedStatement[1];
        final Answer<DelegatingConnection> answer = (inv) -> new DelegatingConnection((Connection) inv.callRealMethod()) {
            @Override
            public PreparedStatement prepareStatement(String sql) throws SQLException {
                spyStmt[0] = spy(getDelegate().prepareStatement(sql));
                return spyStmt[0];
            }
        };
        doAnswer(answer).when(dbcp).getConnection(ArgumentMatchers.anyMap());
        return () -> spyStmt[0];
    }

    private Supplier<Statement> createStatementSpy() {
        final Statement[] spyStmt = new Statement[1];
        final Answer<DelegatingConnection> answer = (inv) -> new DelegatingConnection((Connection) inv.callRealMethod()) {
            @Override
            public Statement createStatement() throws SQLException {
                spyStmt[0] = spy(getDelegate().createStatement());
                return spyStmt[0];
            }
        };
        doAnswer(answer).when(dbcp).getConnection();
        return () -> spyStmt[0];
    }

    static class PutDatabaseRecordUnmatchedField extends BulkInsertClickhouse {
        @Override
        SqlAndIncludedColumns generateInsertFromTableSchema(String tableName, TableSchema tableSchema, DMLSettings settings, NameNormalizer normalizer) throws IllegalArgumentException {
            return new SqlAndIncludedColumns("INSERT INTO PERSONS VALUES (?,?,?,?)", tableSchema.getColumns().keySet().stream().toList());
        }
    }

    static class DBCPServiceThrowConnectionException extends AbstractControllerService implements DBCPService {
        private final Exception rootCause;

        public DBCPServiceThrowConnectionException(final Exception rootCause) {
            this.rootCause = rootCause;
        }

        @Override
        public String getIdentifier() {
            return DBCP_SERVICE_ID;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            throw new ProcessException(rootCause);
        }
    }

    static class DBCPServiceAutoCommitTest extends AbstractControllerService implements DBCPService {
        private final String databaseLocation;

        public DBCPServiceAutoCommitTest(final String databaseLocation) {
            this.databaseLocation = databaseLocation;
        }

        @Override
        public String getIdentifier() {
            return DBCP_SERVICE_ID;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Connection spyConnection = spy(DriverManager.getConnection("jdbc:derby:" + databaseLocation + ";create=true"));
                doThrow(SQLFeatureNotSupportedException.class).when(spyConnection).setAutoCommit(false);
                return spyConnection;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    public static class TestCase {
        TestCase(Boolean autoCommit, Boolean rollbackOnFailure, Integer batchSize) {
            this.autoCommit = autoCommit;
            this.rollbackOnFailure = rollbackOnFailure;
            this.batchSize = batchSize;
        }

        private Boolean autoCommit = null;
        private Boolean rollbackOnFailure = null;
        private Integer batchSize = null;

        String getAutoCommitAsString() {
            return autoCommit == null ? null : autoCommit.toString();
        }

        String getRollbackOnFailureAsString() {
            return rollbackOnFailure == null ? null : rollbackOnFailure.toString();
        }

        String getBatchSizeAsString() {
            return batchSize == null ? null : batchSize.toString();
        }

        @Override
        public String toString() {
            return "autoCommit=" + autoCommit +
                    "; rollbackOnFailure=" + rollbackOnFailure +
                    "; batchSize=" + batchSize;
        }
    }
}

