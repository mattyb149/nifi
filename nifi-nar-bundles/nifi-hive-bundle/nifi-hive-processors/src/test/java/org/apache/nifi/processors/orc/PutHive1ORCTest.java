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
package org.apache.nifi.processors.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.BasicConfigurator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.AbstractHadoopProcessor;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.exception.FailureException;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class PutHive1ORCTest {

    private static final String DIRECTORY = "target";
    private static final String TEST_CONF_PATH = "src/test/resources/core-site.xml";

    private RecordSchema recordSchema;
    private Configuration testConf;
    private PutHive1ORC proc;
    private TestRunner testRunner;

    @BeforeClass
    public static void setupLogging() {
        BasicConfigurator.configure();
    }

    @Before
    public void setup() throws IOException {
        List<RecordField> recordFields = new ArrayList<>(4);
        recordFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField("favorite_number", RecordFieldType.INT.getDataType()));
        recordFields.add(new RecordField("favorite_color", RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField("scale", RecordFieldType.DOUBLE.getDataType()));
        recordSchema = new SimpleRecordSchema(recordFields);
        testConf = new Configuration();
        testConf.addResource(new Path(TEST_CONF_PATH));

        proc = new PutHive1ORC();
    }

    private void configure(final PutHive1ORC PutHive1ORC, final int numUsers) throws InitializationException {
        configure(PutHive1ORC, numUsers, null);
    }

    private void configure(final PutHive1ORC PutHive1ORC, final int numUsers,
                           final BiFunction<Integer, MockRecordParser, Void> recordGenerator)
            throws InitializationException {
        testRunner = TestRunners.newTestRunner(PutHive1ORC);
        testRunner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, TEST_CONF_PATH);
        testRunner.setProperty(AbstractHadoopProcessor.DIRECTORY, DIRECTORY);

        MockRecordParser readerFactory = new MockRecordParser();

        for (final RecordField recordField : recordSchema.getFields()) {
            readerFactory.addSchemaField(recordField);
        }

        if (recordGenerator == null) {
            for (int i = 0; i < numUsers; i++) {
                readerFactory.addRecord("name" + i, i, "blue" + i, i * 10.0);
            }
        } else {
            recordGenerator.apply(numUsers, readerFactory);
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);

        testRunner.setProperty(AbstractPutHDFSRecord.RECORD_READER, "mock-reader-factory");
    }

    @Test
    public void testWriteORCWithDefaults() throws IOException, InitializationException {
        List<RecordField> recordFields = new ArrayList<>();
        recordFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField("favorite_number", RecordFieldType.INT.getDataType()));
        recordFields.add(new RecordField("favorite_color", RecordFieldType.STRING.getDataType()));
        recordFields.add(new RecordField("scale", RecordFieldType.DOUBLE.getDataType()));
        recordSchema = new SimpleRecordSchema(recordFields);

        final String filename = "testORCWithDefaults-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        configure(proc, 100, (numUsers, readerFactory) -> {
            for (int i = 0; i < numUsers; i++) {
                readerFactory.addRecord(
                        "name" + i,
                        i,
                        "blue" + i,
                        i * 10.0);
            }
            return null;
        });

        testRunner.setProperty(PutHive1ORC.HIVE_TABLE_NAME, "myTable");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutHive1ORC.REL_SUCCESS, 1);

        final Path orcFile = new Path(DIRECTORY + "/" + filename);

        // verify the successful flow file has the expected attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutHive1ORC.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(PutHive1ORC.ABSOLUTE_HDFS_PATH_ATTRIBUTE, orcFile.getParent().toString());
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutHive1ORC.RECORD_COUNT_ATTR, "100");
        //mockFlowFile.assertAttributeEquals(PutHive1ORC.HIVE_DDL_ATTRIBUTE,
        //        "CREATE EXTERNAL TABLE IF NOT EXISTS myTable (name STRING, favorite_number INT, favorite_color STRING, scale DOUBLE) STORED AS ORC");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        Assert.assertTrue(provEvent.getTransitUri().endsWith(DIRECTORY + "/" + filename));

        // verify the content of the ORC file by reading it back in
        Reader reader = OrcFile.createReader(orcFile, OrcFile.readerOptions(testConf));
        RecordReader recordReader = reader.rows();

        TypeInfo typeInfo =
                TypeInfoUtils.getTypeInfoFromTypeString("struct<name:string,favorite_number:int,favorite_color:string,scale:double>");
        StructObjectInspector inspector = (StructObjectInspector)
                OrcStruct.createObjectInspector(typeInfo);

        Object nextRecord = null;
        int currUser = 0;
        for (; currUser < 100; currUser++) {
            nextRecord = recordReader.next(nextRecord);
            Assert.assertNotNull(nextRecord);
            Assert.assertTrue("Not an OrcStruct", nextRecord instanceof OrcStruct);
            List<Object> x = inspector.getStructFieldsDataAsList(nextRecord);

            assertEquals("name" + currUser, x.get(0).toString());
            assertEquals(currUser, ((IntWritable) x.get(1)).get());
            assertEquals("blue" + currUser, x.get(2).toString());
            assertEquals(currUser * 10.0, ((DoubleWritable) x.get(3)).get(), Double.MIN_VALUE);
        }
        assertEquals(100, currUser);
        try {
            recordReader.next(nextRecord);
            fail("All records have been read, should have thrown an IOException!");
        } catch (IOException e) {
            // Expected
        }

        // verify we don't have the temp dot file after success
        final File tempOrcFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempOrcFile.exists());

        // verify we DO have the CRC file after success
        final File crcAvroORCFile = new File(DIRECTORY + "/." + filename + ".crc");
        Assert.assertTrue(crcAvroORCFile.exists());
    }

    @Test
    public void testWriteORCWithAvroLogicalTypes() throws IOException, InitializationException {
        List<RecordField> recordFields = new ArrayList<>(5);
        recordFields.add(new RecordField("id", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.INT.getDataType())));
        recordFields.add(new RecordField("timeMillis", RecordFieldType.TIME.getDataType()));
        recordFields.add(new RecordField("timestampMillis", RecordFieldType.TIMESTAMP.getDataType()));
        recordFields.add(new RecordField("dt", RecordFieldType.DATE.getDataType()));
        recordFields.add(new RecordField("dec", RecordFieldType.DOUBLE.getDataType()));
        recordSchema = new SimpleRecordSchema(recordFields);
        Calendar now = Calendar.getInstance();
        LocalTime nowTime = LocalTime.now();
        LocalDateTime nowDateTime = LocalDateTime.now();
        LocalDate epoch = LocalDate.ofEpochDay(0);
        LocalDate nowDate = LocalDate.now();

        final int timeMillis = nowTime.get(ChronoField.MILLI_OF_DAY);
        final Timestamp timestampMillis = Timestamp.valueOf(nowDateTime);
        final Date dt = Date.valueOf(nowDate);
        final double dec = 1234.56;

        configure(proc, 10, (numUsers, readerFactory) -> {
            for (int i = 0; i < numUsers; i++) {
                readerFactory.addRecord(
                        i,
                        timeMillis,
                        timestampMillis,
                        dt,
                        dec);
            }
            return null;
        });

        final String filename = "testORCWithDefaults-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.setProperty(PutHive1ORC.HIVE_TABLE_NAME, "myTable");

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutHive1ORC.REL_SUCCESS, 1);

        final Path orcFile = new Path(DIRECTORY + "/" + filename);

        // verify the successful flow file has the expected attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutHive1ORC.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(PutHive1ORC.ABSOLUTE_HDFS_PATH_ATTRIBUTE, orcFile.getParent().toString());
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutHive1ORC.RECORD_COUNT_ATTR, "10");
        // DDL will be created with field names normalized (lowercased, e.g.) for Hive by default
        mockFlowFile.assertAttributeEquals(PutHive1ORC.HIVE_DDL_ATTRIBUTE,
                "CREATE EXTERNAL TABLE IF NOT EXISTS myTable (id INT, timemillis INT, timestampmillis TIMESTAMP, dt DATE, dec DOUBLE) STORED AS ORC");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        Assert.assertTrue(provEvent.getTransitUri().endsWith(DIRECTORY + "/" + filename));

        // verify the content of the ORC file by reading it back in
        Reader reader = OrcFile.createReader(orcFile, OrcFile.readerOptions(testConf));
        RecordReader recordReader = reader.rows();

        TypeInfo typeInfo =
                TypeInfoUtils.getTypeInfoFromTypeString("struct<id:int,timemillis:int,timestampmillis:timestamp,dt:date,dec:double>");
        StructObjectInspector inspector = (StructObjectInspector)
                OrcStruct.createObjectInspector(typeInfo);

        Object nextRecord = null;
        int currUser = 0;
        for (; currUser < 10; currUser++) {
            nextRecord = recordReader.next(nextRecord);
            Assert.assertNotNull(nextRecord);
            Assert.assertTrue("Not an OrcStruct", nextRecord instanceof OrcStruct);
            List<Object> x = inspector.getStructFieldsDataAsList(nextRecord);

            assertEquals(currUser, ((IntWritable) x.get(0)).get());
            assertEquals(timeMillis, ((IntWritable) x.get(1)).get());
            assertEquals(timestampMillis.getTime(), ((TimestampWritable) x.get(2)).getTimestamp().getTime());
            assertEquals(dt.toLocalDate().toEpochDay(), ((DateWritable) x.get(3)).getDays());
            assertEquals(dec, ((DoubleWritable) x.get(4)).get(), Double.MIN_VALUE);
        }
        assertEquals(10, currUser);


        // verify we don't have the temp dot file after success
        final File tempOrcFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempOrcFile.exists());

        // verify we DO have the CRC file after success
        final File crcAvroORCFile = new File(DIRECTORY + "/." + filename + ".crc");
        Assert.assertTrue(crcAvroORCFile.exists());
    }

    @Test
    public void testMalformedRecordExceptionFromReaderShouldRouteToFailure() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        configure(proc, 10);

        final org.apache.nifi.serialization.RecordReader recordReader = Mockito.mock(org.apache.nifi.serialization.RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutHive1ORC.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutHive1ORC.REL_FAILURE, 1);
    }

    @Test
    public void testIOExceptionCreatingWriterShouldRouteToRetry() throws InitializationException {
        final PutHive1ORC proc = new PutHive1ORC() {
            @Override
            public HDFSRecordWriter createHDFSRecordWriter(ProcessContext context, FlowFile flowFile, Configuration conf, Path path, RecordSchema schema)
                    throws IOException {
                throw new IOException("IOException");
            }
        };
        configure(proc, 0);

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutHive1ORC.REL_RETRY, 1);
    }

    @Test
    public void testIOExceptionFromReaderShouldRouteToRetry() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        configure(proc, 10);

        final RecordSet recordSet = Mockito.mock(RecordSet.class);
        when(recordSet.next()).thenThrow(new IOException("ERROR"));

        final org.apache.nifi.serialization.RecordReader recordReader = Mockito.mock(org.apache.nifi.serialization.RecordReader.class);
        when(recordReader.createRecordSet()).thenReturn(recordSet);
        when(recordReader.getSchema()).thenReturn(recordSchema);

        final RecordReaderFactory readerFactory = Mockito.mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutHive1ORC.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutHive1ORC.REL_RETRY, 1);
    }

    @Test
    public void testIOExceptionRenamingShouldRouteToRetry() throws InitializationException {
        final PutHive1ORC proc = new PutHive1ORC() {
            @Override
            protected void rename(FileSystem fileSystem, Path srcFile, Path destFile)
                    throws IOException, InterruptedException, FailureException {
                throw new IOException("IOException renaming");
            }
        };

        configure(proc, 10);

        final String filename = "testIOExceptionRenamingShouldRouteToRetry-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutHive1ORC.REL_RETRY, 1);

        // verify we don't have the temp dot file after success
        final File tempAvroORCFile = new File(DIRECTORY + "/." + filename);
        Assert.assertFalse(tempAvroORCFile.exists());
    }

    private void verifyORCUsers(final Path orcUsers, final int numExpectedUsers) throws IOException {
        verifyORCUsers(orcUsers, numExpectedUsers, null);
    }

    private void verifyORCUsers(final Path orcUsers, final int numExpectedUsers, BiFunction<List<Object>, Integer, Void> assertFunction) throws IOException {
        Reader reader = OrcFile.createReader(orcUsers, OrcFile.readerOptions(testConf));
        RecordReader recordReader = reader.rows();

        TypeInfo typeInfo =
                TypeInfoUtils.getTypeInfoFromTypeString("struct<name:string,favorite_number:int,favorite_color:string,scale:double>");
        StructObjectInspector inspector = (StructObjectInspector)
                OrcStruct.createObjectInspector(typeInfo);

        int currUser = 0;
        Object nextRecord = null;
        while ((nextRecord = recordReader.next(nextRecord)) != null) {
            Assert.assertNotNull(nextRecord);
            Assert.assertTrue("Not an OrcStruct", nextRecord instanceof OrcStruct);
            List<Object> x = inspector.getStructFieldsDataAsList(nextRecord);

            if (assertFunction == null) {
                assertEquals("name" + currUser, x.get(0).toString());
                assertEquals(currUser, ((IntWritable) x.get(1)).get());
                assertEquals("blue" + currUser, x.get(2).toString());
                assertEquals(10.0 * currUser, ((DoubleWritable) x.get(3)).get(), Double.MIN_VALUE);
            } else {
                assertFunction.apply(x, currUser);
            }
            currUser++;
        }

        assertEquals(numExpectedUsers, currUser);
    }

}