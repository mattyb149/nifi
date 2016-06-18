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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.StopWatch;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "select", "insert", "jdbc", "query", "database", "xml"})
@Stateful(scopes = {Scope.LOCAL}, description = "Database fields can be saved as state info using the 'State Fields' property")
@CapabilityDescription("This processor connect to and backup a database.")
@WritesAttribute(attribute = "¿any?", description = "This processor may write zero or more attributes")
public class JdbcDBBackupGenRunTargetQueries extends AbstractProcessor {
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure to complete FlowFile.")
            .build();

    private static final Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_TARGET_DB = new PropertyDescriptor.Builder()
            .name("Database Connection Target")
            .description("The Controller Service that is used to obtain connection to the target database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    private static final List<PropertyDescriptor> propDescriptors;

    static {
        final Set<Relationship> r = new HashSet<>();
        r.add(FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_TARGET_DB);
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
        final DBCPService dbcpOutgoingService = context.getProperty(DBCP_TARGET_DB).asControllerService(DBCPService.class);
        final StopWatch stopWatch = new StopWatch(true);

        FlowFile file = null;
        Connection tar_conn = null;
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
        String tar_database = f_attr.get("target_database");
        String tar_schema = f_attr.get("target_schema");
        String tar_table_name = f_attr.get("target_table_name");
        String primary_key = f_attr.get("source_primary_key");

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
        List<String> resultSet = new ArrayList<>(); // This should contain all of the records the source query generated
        String columns = ""; // This should contain all of the column names from the source query (these should be the same as the target column names)
        String dataType = "";
        String results = "";
        Document doc = null;
        session.remove(file);
        /* We need to figure out how to handle the CLOBs here, our errors are occurring here with the CLOB data. */
        try {
            DocumentBuilderFactory dbfact = DocumentBuilderFactory.newInstance();
            DocumentBuilder dbuilder = dbfact.newDocumentBuilder();
            doc = dbuilder.parse(new ByteArrayInputStream(xml_results.getBytes()));
            NodeList sql_record = doc.getElementsByTagName("record");
            Node rec = sql_record.item(0);
            results = rec.getTextContent();
            dataType = rec.getAttributes().getNamedItem("dataType").getNodeValue();
        } catch (Exception e) {
            logger.error("Something went wrong...");
            context.yield();
            throw new ProcessException("Error in xml_results parsing: ", e);
        }

        if (dataType.equals("CSV")) {
            NodeList sql_cols = doc.getElementsByTagName("columnNames");
            Node cols = sql_cols.item(0);
            columns = cols.getTextContent();
            resultSet = Arrays.asList(results.split("\\n"));
        } else if (dataType.equals("XML")) {
            try {
                DocumentBuilderFactory dbfact = DocumentBuilderFactory.newInstance();
                DocumentBuilder dbuilder = dbfact.newDocumentBuilder();
                doc = dbuilder.parse(new ByteArrayInputStream(xml_results.getBytes()));
                NodeList sql_record = doc.getElementsByTagName("record");
                StringBuilder col_names = new StringBuilder();
                StringBuilder result = new StringBuilder();
                NodeList record = sql_record.item(0).getChildNodes();
                for (int i = 0; i < record.getLength(); i++) {
                    Node currentNode = record.item(i);
                    col_names.append(currentNode.getNodeName()).append(",");
                    result.append(currentNode.getTextContent());
                }
                columns = col_names.toString();
                columns = columns.substring(0, columns.length() - 1);
                resultSet.add(result.toString());
            } catch (Exception e) {
                logger.error("Something went wrong...");
                context.yield();
                throw new ProcessException("Error in XML Parsing: ", e);
            }
        }

        StringBuilder sql_query = new StringBuilder();
        String query = "";
        if (SQL_query_type.equals("insert")) {
            sql_query.append("INSERT INTO ")
                    .append(tar_table_name)
                    .append(" (")
                    .append(columns)
                    .append(") VALUES ");
            for (String row : resultSet) {
                sql_query.append("(").append(row).append("),");
            }
            query = sql_query.toString();
            query = query.substring(0, query.length() - 1);
            query = query + ";";
        } else {
            String update_id = f_attr.get("update_id");
            // use primary key to update a record
            sql_query.append("UPDATE ").append(tar_table_name).append(" SET ");
            List<String> col_array = Arrays.asList(columns.split(","));
            for (int i = 0; i < col_array.size(); i++) {
                sql_query.append(col_array.get(i)).append("=").append(resultSet.get(i)).append(",");
            }
            query = sql_query.toString();
            query = query.substring(0, query.length() - 1);
            query = query + " WHERE " + primary_key + "=" + update_id + ";";
        }

        //Now run target queries
        try {
            String use_query = "USE " + tar_database + ";";
            tar_conn = dbcpOutgoingService.getConnection();
            Statement tar_stat = tar_conn.createStatement();
            tar_stat.execute(use_query);
            tar_stat.execute(query);
            tar_stat.close();
            tar_conn.close();
        } catch (Exception e) {
            logger.error("Something went wrong...");
            context.yield();
            throw new ProcessException("Error in pushing to target database: ", e);
        }
    }
}
