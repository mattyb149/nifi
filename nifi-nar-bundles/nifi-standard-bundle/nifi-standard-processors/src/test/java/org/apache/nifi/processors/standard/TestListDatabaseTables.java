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
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


/**
 * Unit tests for ListDatabaseTables processor.
 */
public class TestListDatabaseTables {

    TestRunner runner;
    ListDatabaseTables processor;

    private final static String DB_LOCATION = "target/db_ldt";

    @BeforeClass
    public static void setupBeforeClass() throws IOException {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (SQLNonTransientConnectionException e) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @Before
    public void setUp() throws Exception {
        processor = new ListDatabaseTables();
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ListDatabaseTables.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ListDatabaseTables.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testListTablesNoCount() throws Exception {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        // Already got these tables, shouldn't get them again
        runner.clearTransferState();
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 0);
    }

    @Test
    public void testListTablesWithCount() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        // Make sure it doesn't get the same tables
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("0", results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
    }

    @Test
    public void testListTablesWithCountAndMaxVals() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.INCLUDE_MAX_VALUES, "true");

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 4, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (3, 2, 2)");

        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        // Initial flow files sent as well as those with max values set
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 4);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        MockFlowFile firstFlowFile = results.get(1);
        assertEquals("3", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("3", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "ID" + ".value"));
        assertEquals("4", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "VAL1" + ".value"));
        assertEquals("2", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "VAL2" + ".value"));
        assertEquals("0", results.get(3).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
    }

    @Test
    public void testListTablesWithNoCountAndMaxVals() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "false");
        runner.setProperty(ListDatabaseTables.INCLUDE_MAX_VALUES, "true");

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 4, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (3, 2, 2)");

        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        // Initial flow files sent as well as those with max values set
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 4);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        MockFlowFile firstFlowFile = results.get(1);
        assertNull(firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("3", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "ID" + ".value"));
        assertEquals("4", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "ID" + ".type"));
        assertEquals("4", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "VAL1" + ".value"));
        assertEquals("2", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "VAL2" + ".value"));
        assertNull(results.get(3).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
    }

    @Test
    public void testListTablesWithNoCountAndMaxValsNoInitFlowFile() throws Exception {
        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "false");
        runner.setProperty(ListDatabaseTables.INCLUDE_MAX_VALUES, "true");
        runner.setProperty(ListDatabaseTables.GENERATE_INITIAL_FLOWFILE, "false");

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 4, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (3, 2, 2)");

        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        MockFlowFile firstFlowFile = results.get(0);
        assertNull(firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("3", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "ID" + ".value"));
        assertEquals("4", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "ID" + ".type"));
        assertEquals("4", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "VAL1" + ".value"));
        assertEquals("2", firstFlowFile.getAttribute(ListDatabaseTables.DB_TABLE_MAXVAL_PREFIX + "VAL2" + ".value"));
        assertNull(results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
    }

    @Test
    public void testListTablesAfterRefresh() throws Exception {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TABLE1");
            stmt.execute("drop table TEST_TABLE2");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_TABLE1 (id integer not null, val1 integer, val2 integer, constraint my_pk1 primary key (id))");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TABLE1 (id, val1, val2) VALUES (1, 1, 1)");
        stmt.execute("create table TEST_TABLE2 (id integer not null, val1 integer, val2 integer, constraint my_pk2 primary key (id))");

        runner.setProperty(ListDatabaseTables.INCLUDE_COUNT, "true");
        runner.setProperty(ListDatabaseTables.REFRESH_INTERVAL, "100 millis");
        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ListDatabaseTables.REL_SUCCESS);
        assertEquals("2", results.get(0).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        assertEquals("0", results.get(1).getAttribute(ListDatabaseTables.DB_TABLE_COUNT));
        runner.clearTransferState();
        Thread.sleep(200);

        runner.run();
        runner.assertTransferCount(ListDatabaseTables.REL_SUCCESS, 2);
    }

    /**
     * Simple implementation only for ListDatabaseTables processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}