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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.StopWatch;


@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "select", "insert", "jdbc", "query", "database", "xml"})
@Stateful(scopes = {Scope.LOCAL}, description = "Database fields can be saved as state info using the 'State Fields' property")
@CapabilityDescription("This processor connect to and backup a database.")
@WritesAttribute(attribute = "¿any?", description = "This processor can be configured to create any number of attributes using the 'Attribute Fields' property.")
public class JdbcDBBackupGenSourceQueries extends AbstractSessionFactoryProcessor {
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully completed step, move to next step.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Execution failed, discontinue process.")
            .build();

    private static final Set<Relationship> relationships;

    static {
        final Set<Relationship> r = new HashSet<>();
        r.add(SUCCESS);
        r.add(FAILURE);
        relationships = Collections.unmodifiableSet(r);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        ProcessSession resultsSession = sessionFactory.createSession();

        FlowFile file = null;
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
        String SQL_query_call = f_attr.get("sql_query_call");
//        String src_database_engine = f_attr.get("source_database_engine"); // This will be used in the future
        String src_database = f_attr.get("source_database");
        String src_schema = f_attr.get("source_schema");
        String src_table_name = f_attr.get("source_table_name");
        Integer src_record_count = Integer.parseInt(f_attr.get("source_record_count"));
        String src_primary_key = f_attr.get("source_primary_key");
        Integer limit = Integer.parseInt(f_attr.get("limit_counter"));
        session.remove(file);
        /* table_dependencies will be used in the future, need some way to check flowfiles that have succeeded
         * to ensure that certain files got picked up and executed first - Or use the Incrementor and Migrator
         * to control what gets sent through the data flow first, versus second. */
//        String table_dependencies = f_attr.get("Table Dependencies");
//        String exclude_tables = f_attr.get("Exclude Tables"); // This should only be necessary in the incrementor and migrator
        if (SQL_query_call.equals("incrementor")) {
            String SQL_query_type = f_attr.get("sql_query_type");
            String src_id_string = f_attr.get("source_id_string");
            List<String> src_ids = Arrays.asList(src_id_string.split(","));
            if (SQL_query_type.equals("insert")) {
                // Split source strings into groups of limit amounts
                List<Integer> sorted = new ArrayList<>();
                for (String id : src_ids) {
                    sorted.add(Integer.parseInt(id));
                }
                Collections.sort(sorted);
                src_ids = new ArrayList<>();
                for (Integer id : sorted) {
                    src_ids.add(Integer.toString(id));
                }
                Map<Integer, List<String>> master_id_list = new HashMap<>();
                Integer count = new Integer(1);
                Integer st = new Integer(0);
                Integer en = new Integer(limit);
                while (count <= (src_record_count / limit)) {
                    List<String> temp = new ArrayList<>();
                    temp = src_ids.subList(st, en);
                    master_id_list.put(count, temp);
                    st = new Integer(en);
                    en = new Integer(en + limit);
                    count++;
                }
                // Use string builder to compile the string before doing a single write
                for (List<String> id_list : master_id_list.values()) {
                    final StringBuilder sql_statements = new StringBuilder();
                    FlowFile result_file = resultsSession.create();
                    sql_statements.append("<SQLStatements><statement><sql>USE [" + src_database + "];</sql><note>run_1|no_results</note></statement>");
                    String t_start = id_list.get(0);
                    String t_end = id_list.get(id_list.size() - 1);
                    String insertStatement = "<statement><sql>SELECT * FROM " + src_schema + "." + src_table_name + " WHERE " + src_primary_key + " IN (" + src_id_string + ") "
                            + "AND " + src_primary_key + " BETWEEN " + t_start + " AND " + t_end + " ORDER BY " + src_primary_key + " ASC;</sql><note>run_2|get_results</note></statement>";
                    sql_statements.append(insertStatement);
                    sql_statements.append("</SQLStatements>");
                    result_file = resultsSession.putAllAttributes(result_file, f_attr);
                    result_file = resultsSession.write(result_file, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(sql_statements.toString().getBytes("ISO8859_1"));
                        }
                    });
                    resultsSession.getProvenanceReporter().receive(result_file, "Incrementor - Created insert statements: ", stopWatch.getElapsed(TimeUnit.SECONDS));
                    resultsSession.transfer(result_file, SUCCESS);
                    resultsSession.commit();
                    System.gc();
                }
            } else if (SQL_query_type.equals("update")) {
                // Update queries
                for (String id : src_ids) {
                    final StringBuilder sql_statements = new StringBuilder();
                    FlowFile result_file = resultsSession.create();
                    sql_statements.append("<SQLStatements><statement><sql>USE [" + src_database + "];</sql><note>run_1|no_results</note></statement>");
                    String updateStatement = "<statement><sql>SELECT * FROM " + src_schema + "." + src_table_name + " WHERE " + src_primary_key + "=" + id + ";</sql>"
                            + "<note>run_2|get_results</note></statement>";
                    sql_statements.append(updateStatement);
                    sql_statements.append("</SQLStatements>");
                    result_file = resultsSession.putAllAttributes(result_file, f_attr);
                    result_file = resultsSession.putAttribute(result_file, "update_id", id);
                    result_file = resultsSession.write(result_file, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(sql_statements.toString().getBytes("ISO8859_1"));
                        }
                    });
                    resultsSession.getProvenanceReporter().receive(result_file, "Incrementor - Created update statements: ", stopWatch.getElapsed(TimeUnit.SECONDS));
                    resultsSession.transfer(result_file, SUCCESS);
                    resultsSession.commit();
                    System.gc();
                }
            } else {
                // Delete queries don't need to be run on the source system, so probably pass on through here and just keep the configs the same.
                final StringBuilder sql_statements = new StringBuilder();
                sql_statements.append("<SQLStatements><statement><note>run_null|no_run</note></statement>");
                sql_statements.append("</SQLStatements>");
                file = session.write(file, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(sql_statements.toString().getBytes("ISO8859_1"));
                    }
                });
                session.getProvenanceReporter().receive(file, "Incrementor - Source does not generate delete queries. ", stopWatch.getElapsed(TimeUnit.SECONDS));
                session.transfer(file, SUCCESS);
                session.commit();
                System.gc();
            }
            session.remove(file);
        } else {
            // Query type is only insert with the migrator
            Integer target_record_count = Integer.parseInt(f_attr.get("target_record_count"));
            Integer t_start = new Integer(0);
            if (target_record_count > 0) {
                t_start = new Integer(target_record_count + 1);
            }
            Integer t_end = new Integer(limit);
            if (t_start > 0) {
                if (t_start + limit > src_record_count) {
                    t_end = new Integer(src_record_count);
                } else {
                    t_end = new Integer(t_start + limit);
                }
            } else if (src_record_count < limit) {
                t_end = new Integer(src_record_count);
            }
            // Adjustment needs to be made here, we need to account for interruptions.
            Integer queue_size = new Integer(0);
            while (t_end <= src_record_count) {
                final StringBuilder sql_statements = new StringBuilder();
                FlowFile result_file = resultsSession.create();
                sql_statements.append("<SQLStatements><statement><sql>USE [" + src_database + "];</sql><note>run_1|no_results</note></statement>");
                String insertStatement = "<statement><sql>SELECT * FROM ( SELECT Row_Number() OVER ( ORDER BY " + src_primary_key + " ) AS row_num, * FROM [" + src_table_name + "] ) "
                        + "AS pTable WHERE row_num BETWEEN " + t_start + " AND " + t_end + ";</sql><note>run_2|get_results</note></statement>";
                sql_statements.append(insertStatement);
                sql_statements.append("</SQLStatements>");
                result_file = resultsSession.putAllAttributes(result_file, f_attr);
                result_file = resultsSession.write(result_file, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(sql_statements.toString().getBytes("ISO8859_1"));
                    }
                });
                if (queue_size < 2000) {
                    resultsSession.getProvenanceReporter().receive(result_file, "Migrator - Created insert statements: ", stopWatch.getElapsed(TimeUnit.SECONDS));
                    resultsSession.transfer(result_file, SUCCESS);
                    resultsSession.commit();
                    t_start = new Integer(t_end + 1);
                    t_end = new Integer(t_end + limit);
                    System.gc();
                } else {
                    try {
                        TimeUnit.MINUTES.sleep(10);
                        queue_size = new Integer(0);
                        resultsSession.getProvenanceReporter().receive(result_file, "Migrator - Created insert statements: ", stopWatch.getElapsed(TimeUnit.SECONDS));
                        resultsSession.transfer(result_file, SUCCESS);
                        resultsSession.commit();
                        t_start = new Integer(t_end + 1);
                        t_end = new Integer(t_end + limit);
                    } catch (InterruptedException e) {
                        logger.error("Processor interrupted.");
                        context.yield();
                        throw new ProcessException("Processor interrupted: ", e);
                    }
                }
                queue_size++;
            }
            session.remove(file);
        }
    }
}
