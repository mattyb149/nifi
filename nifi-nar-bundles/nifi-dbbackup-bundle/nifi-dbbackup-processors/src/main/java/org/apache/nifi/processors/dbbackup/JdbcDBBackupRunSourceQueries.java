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
package org.apache.nifi.processors.dbbackup;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.StopWatch;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "select", "insert", "jdbc", "query", "database", "xml"})
@Stateful(scopes = {Scope.LOCAL}, description = "Database fields can be saved as state info using the 'State Fields' property")
@CapabilityDescription("This processor connect to and backup a database.")
@WritesAttribute(attribute = "¿any?", description = "This processor may write zero or more attributes")
public class JdbcDBBackupRunSourceQueries extends AbstractProcessor {
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    private static final Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SOURCE_DB = new PropertyDescriptor.Builder()
            .name("Database Connection Source")
            .description("The Controller Service that is used to obtain connection to the source database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    private static final List<PropertyDescriptor> propDescriptors;

    static {
        final Set<Relationship> r = new HashSet<>();
        r.add(SUCCESS);
        r.add(FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SOURCE_DB);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final DBCPService dbcpIncomingService = context.getProperty(DBCP_SOURCE_DB).asControllerService(DBCPService.class);
        final StopWatch stopWatch = new StopWatch(true);

        FlowFile file = null;
        Connection src_conn = null;
        if (context.hasIncomingConnection()) {
            file = session.get();

            /* If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
             * However, if we have no FlowFile and we have connections coming from other Processors, then
             * we know that we should run only if we have a FlowFile. */
            if (file == null && context.hasNonLoopConnection()) {
                session.transfer(file, FAILURE);
                logger.error("No flow file found, throwing error.");
                context.yield();
            }
        }

        Map<String, String> f_attr = file.getAttributes();
        String SQL_query_type = f_attr.get("sql_query_type");

        final AtomicReference<String> flowfile_contents = new AtomicReference<>();
        session.read(file, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    String xml = IOUtils.toString(in);
                    flowfile_contents.set(xml);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("Failed to get flow file contents.");
                }
            }
        });
        String xml_results = flowfile_contents.get();
        if (xml_results == null || xml_results.isEmpty()) {
            logger.error("XML read failed.");
            context.yield();
            throw new ProcessException("Unable to read XML");
        }
        Map<String, String> sql_command = new HashMap<>();
        // Map<String, Boolean> run_order = new HashMap<>(); Add this in another time
        try {
            session.remove(file);
        } catch (FlowFileHandlingException e) {
            logger.error("File has not been removed");
            context.yield();
            throw new ProcessException("Error in FlowFile handling: ", e);
        }
        try {
            DocumentBuilderFactory dbfact = DocumentBuilderFactory.newInstance();
            DocumentBuilder dbuilder = dbfact.newDocumentBuilder();
            Document doc = dbuilder.parse(new ByteArrayInputStream(xml_results.getBytes()));
            NodeList sql_statements = doc.getElementsByTagName("statement");

            for (int ind = 0; ind < sql_statements.getLength(); ind++) {
                Node statement = sql_statements.item(ind);
                if (statement.getNodeType() == Node.ELEMENT_NODE) {
                    Element sql_or_note = (Element) statement;
                    String cmd = sql_or_note.getElementsByTagName("sql").item(0).getTextContent();
                    String note = sql_or_note.getElementsByTagName("note").item(0).getTextContent();
                    sql_command.put(note, cmd);
                    // List<String> temp = Arrays.asList( note.split("|") );
                    // run_order.put(temp.get(0), false);
                }
            }
        } catch (Exception e) {
            logger.error("Something went wrong...");
            context.yield();
            throw new ProcessException("Error in XML Parsing: ", e);
        }

        try {
            src_conn = dbcpIncomingService.getConnection();
            FlowFile result_file = session.create();
            result_file = session.putAllAttributes(result_file, f_attr);
            final StringBuilder sql_results = new StringBuilder();
            for (Entry<String, String> sql : sql_command.entrySet()) {
                Statement src_stat = src_conn.createStatement();
                String sql_note = sql.getKey().toString();
                String sql_cmd = sql.getValue();
                List<String> note_params = Arrays.asList(sql_note.split("\\|"));
                /* The sql query type here is needed because we want to do different outputs depending on what was selected
                 * For instance: it is perfectly okay to generate a string or CSV format and send it on it's way for an
                 * update query, but an insert query has a larger volume of data to move through, so it requires different
                 * handling. */
                if (note_params.get(1).equals("get_results")) {
                    ResultSet src_results = src_stat.executeQuery(sql_cmd);
                    if (SQL_query_type.equals("insert")) {
                        // This needs to have a bit of a different treatment than update
                        sql_results.append("<SQLResults><record queryType=\"insert\" dataType=\"CSV\">");
                        ResultSetMetaData src_meta = src_results.getMetaData();
                        Integer column_total = src_meta.getColumnCount();
                        final StringBuilder col_str = new StringBuilder();
                        while (src_results.next()) {
                            final StringBuilder temp_str = new StringBuilder();
                            for (int i = 2; i <= column_total; i++) {
                                try {
                                    Object row_object = src_results.getObject(i);
                                    if (row_object instanceof Clob) {
                                        // This needs to be treated differently, as we'll need to put the object into the xml string
                                        Reader reader = ((Clob) row_object).getCharacterStream();
                                        BufferedReader br = new BufferedReader(reader);
                                        String line;
                                        temp_str.append("\"");
                                        while (null != (line = br.readLine())) {
                                            temp_str.append(StringEscapeUtils.escapeHtml4(StringEscapeUtils.escapeJava(line)));
                                        }
                                        br.close();
                                        temp_str.append("\",");
                                    } else {
                                        temp_str.append("\"" + StringEscapeUtils.escapeHtml4(StringEscapeUtils.escapeJava(row_object.toString())) + "\",");
                                    }
                                } catch (NullPointerException npe) {
                                    temp_str.append("\"null\",");
                                } catch (Exception e) {
                                    logger.error("Error running query: " + sql_cmd);
                                    context.yield();
                                    throw new ProcessException("Error in insert query: ", e);
                                }
                            }
                            String row = temp_str.toString();
                            // This to take off the last comma
                            row = row.substring(0, row.length() - 1);
                            sql_results.append(row);
                            sql_results.append("\n");
                        }
                        for (int i = 2; i <= column_total; i++) {
                            col_str.append(src_meta.getColumnName(i)).append(",");
                        }
                        String column_names = col_str.toString();
                        column_names = column_names.substring(0, column_names.length() - 1);
                        sql_results.append("</record><columnNames>")
                                .append(column_names)
                                .append("</columnNames>")
                                .append("<fromQuery>")
                                .append(xml_results)
                                .append("</fromQuery>")
                                .append("</SQLResults>");
                    } else if (SQL_query_type.equals("update")) {
                        // This needs to have a bit of a different treatment than insert
                        // Convert to string and store in key/value pairs in xml with xml_results
                        sql_results.append("<SQLResults><record queryType=\"update\" dataType=\"XML\">");
                        ResultSetMetaData src_meta = src_results.getMetaData();
                        Integer column_total = src_meta.getColumnCount();
                        while (src_results.next()) {
                            for (int i = 1; i <= column_total; i++) {
                                String columnName = src_meta.getColumnName(i);
                                try {
                                    Object row_object = src_results.getObject(i);
                                    if (row_object instanceof Clob) {
                                        // This needs to be treated differently, as we'll need to put the object into the xml string
                                        sql_results.append("<" + columnName + ">");
                                        Reader reader = ((Clob) row_object).getCharacterStream();
                                        BufferedReader br = new BufferedReader(reader);
                                        String line;
                                        while (null != (line = br.readLine())) {
                                            sql_results.append(StringEscapeUtils.escapeHtml4(StringEscapeUtils.escapeJava(line)));
                                        }
                                        br.close();
                                        sql_results.append("</" + columnName + ">");
                                    } else {
                                        sql_results.append("<" + columnName + ">" + StringEscapeUtils.escapeHtml4(StringEscapeUtils.escapeJava(row_object.toString())) + "</" + columnName + ">");
                                    }
                                } catch (NullPointerException npe) {
                                    sql_results.append("<" + columnName + ">null</" + columnName + ">");
                                } catch (Exception e) {
                                    logger.error("Error running query.");
                                    context.yield();
                                    throw new ProcessException("Error in update query: ", e);
                                }
                            }
                        }
                        sql_results.append("</record>").append("<fromQuery>").append(xml_results).append("</fromQuery>").append("</SQLResults>");
                        result_file = session.write(result_file, new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream out) throws IOException {
                                out.write(sql_results.toString().getBytes("ISO8859_1"));
                            }
                        });
                    }
                } else if (note_params.get(1).equals("no_results")) {
                    // Run the query, but don't expect a result
                    src_stat.execute(sql_cmd);
                } else {
                    // No query to run, parameter should read no_run
                    sql_results.append(xml_results);
                    continue;
                }
                src_stat.close();
            }

            result_file = session.write(result_file, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(sql_results.toString().getBytes("ISO8859_1"));
                }
            });
            session.getProvenanceReporter().receive(result_file, "Source queries have been run, results have been generated. ", stopWatch.getElapsed(TimeUnit.SECONDS));
            session.transfer(result_file, SUCCESS);
            session.commit();
            src_conn.close();
            System.gc();
            context.yield();
        } catch (Exception e) {
            logger.error("Something went wrong... " + e.getMessage());
            context.yield();
            throw new ProcessException("Error running source queries: ", e);
        }
    }
}
