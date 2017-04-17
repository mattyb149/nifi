/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard

import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processors.standard.util.record.MockRecordParser
import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.apache.nifi.util.file.FileUtils
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.Statement

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
import static org.mockito.Mockito.spy

/**
 * Unit tests for the PutDatabaseRecord processor
 */
@RunWith(JUnit4.class)
class TestPutDatabaseRecord {

    private static final String createPersons = "CREATE TABLE PERSONS (id integer primary key, name varchar(100), code integer)"
    private final static String DB_LOCATION = "target/db_pdr"

    TestRunner runner
    PutDatabaseRecord processor
    DBCPServiceSimpleImpl dbcp

    @BeforeClass
    static void setupBeforeClass() throws IOException {
        System.setProperty("derby.stream.error.file", "target/derby.log")

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        try {
            FileUtils.deleteFile(dbLocation, true)
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @AfterClass
    static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true")
        } catch (SQLNonTransientConnectionException ignore) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION)
        try {
            FileUtils.deleteFile(dbLocation, true)
        } catch (IOException ignore) {
            // Do nothing, may not have existed
        }
    }

    @Before
    void setUp() throws Exception {
        processor = new PutDatabaseRecord()
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl(DB_LOCATION))

        final Map<String, String> dbcpProperties = new HashMap<>()

        runner = TestRunners.newTestRunner(processor)
        runner.addControllerService("dbcp", dbcp, dbcpProperties)
        runner.enableControllerService(dbcp)
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "dbcp")
    }

    @Test
    void testGeneratePreparedStatements() throws Exception {

        final List<RecordField> fields = [new RecordField('id', RecordFieldType.INT.dataType),
                      new RecordField('name', RecordFieldType.STRING.dataType),
                      new RecordField('code', RecordFieldType.INT.dataType)]

        def schema = [
                getFields    : {fields},
                getFieldCount: {fields.size()},
                getField     : {int index -> fields[index]},
                getDataTypes : {fields.collect {it.dataType}},
                getFieldNames: {fields.collect {it.fieldName}},
                getDataType  : {fieldName -> fields.find {it.fieldName == fieldName}.dataType}
        ] as RecordSchema

        def tableSchema = [
                [
                        new PutDatabaseRecord.ColumnDescription('id', 4, true, 2),
                        new PutDatabaseRecord.ColumnDescription('name', 12, true, 255),
                        new PutDatabaseRecord.ColumnDescription('code', 4, true, 10)
                ],
                false,
                ['id'] as Set<String>,
                ''

        ] as PutDatabaseRecord.TableSchema

        processor.with {
            try {
                assertNull(generateInsert(null, null, null,
                        false, false, false, false,
                        false, false).sql)
                fail('Expecting ProcessException')
            } catch (ProcessException ignore) {
                // Expected
            }
            try {
                assertNull(generateInsert(null, 'PERSONS', null,
                        false, false, false, false,
                        false, false).sql)
                fail('Expecting ProcessException')
            } catch (ProcessException ignore) {
                // Expected
            }

            assertEquals('INSERT INTO PERSONS (id, name, code) VALUES (?,?,?)',
                    generateInsert(schema, 'PERSONS', tableSchema,
                            false, false, false, false,
                            false, false).sql)

            assertEquals('DELETE FROM PERSONS WHERE id = ? AND name = ? AND code = ?',
                    generateDelete(schema, 'PERSONS', tableSchema,
                            false, false, false, false,
                            false, false).sql)
        }
    }

    @Test
    void testInsert() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)
        parser.addRecord(2, 'rec2', 102)
        parser.addRecord(3, 'rec3', 103)
        parser.addRecord(4, 'rec4', 104)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(4, rs.getInt(1))
        assertEquals('rec4', rs.getString(2))
        assertEquals(104, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testInsertNoTable() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 101)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.INSERT_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, '${not.a.real.attr}')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 0)
        runner.assertTransferCount(PutDatabaseRecord.REL_FAILURE, 1)
    }

    @Test
    void testInsertViaSqlStatementType() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("sql", RecordFieldType.STRING)

        parser.addRecord('''INSERT INTO PERSONS (id, name, code) VALUES (1, 'rec1',101)''')
        parser.addRecord('''INSERT INTO PERSONS (id, name, code) VALUES (2, 'rec2',102)''')

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.USE_ATTR_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')
        runner.setProperty(PutDatabaseRecord.FIELD_CONTAINING_SQL, 'sql')

        def attrs = [:]
        attrs[PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE] = 'sql'
        runner.enqueue(new byte[0], attrs)
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(102, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testUpdate() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(1, 'rec1', 201)
        parser.addRecord(2, 'rec2', 202)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.UPDATE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        // Set some existing records with different values for name and code
        final Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute('''INSERT INTO PERSONS VALUES (1,'x1',101)''')
        stmt.execute('''INSERT INTO PERSONS VALUES (2,'x2',102)''')
        stmt.close()

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(201, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(2, rs.getInt(1))
        assertEquals('rec2', rs.getString(2))
        assertEquals(202, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    @Test
    void testDelete() throws InitializationException, ProcessException, SQLException, IOException {
        recreateTable("PERSONS", createPersons)
        Connection conn = dbcp.getConnection()
        Statement stmt = conn.createStatement()
        stmt.execute("INSERT INTO PERSONS VALUES (1,'rec1', 101)")
        stmt.execute("INSERT INTO PERSONS VALUES (2,'rec2', 102)")
        stmt.execute("INSERT INTO PERSONS VALUES (3,'rec3', 103)")
        stmt.close()

        final MockRecordParser parser = new MockRecordParser()
        runner.addControllerService("parser", parser)
        runner.enableControllerService(parser)

        parser.addSchemaField("id", RecordFieldType.INT)
        parser.addSchemaField("name", RecordFieldType.STRING)
        parser.addSchemaField("code", RecordFieldType.INT)

        parser.addRecord(2, 'rec2', 102)

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, 'parser')
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, PutDatabaseRecord.DELETE_TYPE)
        runner.setProperty(PutDatabaseRecord.TABLE_NAME, 'PERSONS')

        runner.enqueue(new byte[0])
        runner.run()

        runner.assertTransferCount(PutDatabaseRecord.REL_SUCCESS, 1)
        stmt = conn.createStatement()
        final ResultSet rs = stmt.executeQuery('SELECT * FROM PERSONS')
        assertTrue(rs.next())
        assertEquals(1, rs.getInt(1))
        assertEquals('rec1', rs.getString(2))
        assertEquals(101, rs.getInt(3))
        assertTrue(rs.next())
        assertEquals(3, rs.getInt(1))
        assertEquals('rec3', rs.getString(2))
        assertEquals(103, rs.getInt(3))
        assertFalse(rs.next())

        stmt.close()
        conn.close()
    }

    private void recreateTable(String tableName, String createSQL) throws ProcessException, SQLException {
        final Connection conn = dbcp.getConnection()
        final Statement stmt = conn.createStatement()
        try {
            stmt.executeUpdate("drop table " + tableName)
        } catch (SQLException ignore) {
            // Do nothing, may not have existed
        }
        stmt.executeUpdate(createSQL)
        stmt.close()
        conn.close()
    }
}
