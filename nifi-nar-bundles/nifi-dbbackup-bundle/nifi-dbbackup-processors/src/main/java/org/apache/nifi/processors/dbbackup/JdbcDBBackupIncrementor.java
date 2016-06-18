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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "insert", "jdbc", "query", "database", "xml"})
@Stateful(scopes = {Scope.LOCAL}, description = "Database fields can be saved as state info using the 'State Fields' property")
@CapabilityDescription("This processor connect to and backup a database.")
@WritesAttribute(attribute = "¿any?", description = "This processor can be configured to create any number of attributes using the 'Attribute Fields' property.")
public class JdbcDBBackupIncrementor extends AbstractSessionFactoryProcessor {
    public static final Relationship STRUCTURE = new Relationship.Builder()
            .name("structure")
            .description("Successfully created FlowFile for structural changes.")
            .build();

    public static final Relationship DATA = new Relationship.Builder()
            .name("data")
            .description("Successfully created FlowFile for data changes.")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully checked table, no changes needed.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SOURCE_DB = new PropertyDescriptor.Builder()
            .name("Database Connection Source")
            .description("The Controller Service that is used to obtain connection to the source database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor DBCP_TARGET_DB = new PropertyDescriptor.Builder()
            .name("Database Connection Target")
            .description("The Controller Service that is used to obtain connection to the target database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    // These can come from the JdbcDBBackup_genCommon processor
    public static final PropertyDescriptor SOURCE_DATABASE_ENGINE = new PropertyDescriptor.Builder()
            .name("Source Database Engine")
            .description("The database engine that source is running.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_DATABASE = new PropertyDescriptor.Builder()
            .name("Source Database Name")
            .description("The name of the source database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_SCHEMA = new PropertyDescriptor.Builder()
            .name("Source Database Schema Name")
            .description("The name of the source database schema.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final PropertyDescriptor TARGET_DATABASE_ENGINE = new PropertyDescriptor.Builder()
            .name("Target Database Engine")
            .description("The database engine that target is running.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("MariaDB")
            .build();

    public static final PropertyDescriptor TARGET_DATABASE = new PropertyDescriptor.Builder()
            .name("Target Database Name")
            .description("The name of the target database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TARGET_SCHEMA = new PropertyDescriptor.Builder()
            .name("Target Database Schema Name")
            .description("The name of the target database schema.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

//    public static final PropertyDescriptor DATE_COMPARISON_FIELD_NAME = new PropertyDescriptor.Builder()
//            .name("Date Comparison Field Name")
//            .description("The names of the fields we need to compare dates with (CSV format please, the items will be tried in the order they appear in the list).")
//            .required(true)
//            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//            .build();

    public static final PropertyDescriptor TABLE_DEPENDENCIES = new PropertyDescriptor.Builder()
            .name("Table Dependencies")
            .description("Indicates which tables need to be inserted and filled before others.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final PropertyDescriptor EXCLUDE_TABLES = new PropertyDescriptor.Builder()
            .name("Exclude Tables")
            .description("Indicates tables that are not necessary to copy over.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final PropertyDescriptor LIMIT_COUNTER = new PropertyDescriptor.Builder()
            .name("Limit")
            .description("The amount of records we pull out in a single chunk.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor SPECIAL_LIMIT_TABLES = new PropertyDescriptor.Builder()
            .name("Special Limit Tables")
            .description("Tables that have larger datatypes that will need lower limits (table,limit|table,limit).")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    private final static List<PropertyDescriptor> propDescriptors;

    static {
        final Set<Relationship> r = new HashSet<>();
        r.add(STRUCTURE);
        r.add(DATA);
        r.add(SUCCESS);
        r.add(FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SOURCE_DB);
        pds.add(DBCP_TARGET_DB);
        pds.add(SOURCE_DATABASE_ENGINE);
        pds.add(SOURCE_DATABASE);
        pds.add(SOURCE_SCHEMA);
        pds.add(TARGET_DATABASE_ENGINE);
        pds.add(TARGET_DATABASE);
        pds.add(TARGET_SCHEMA);
        // pds.add(DATE_COMPARISON_FIELD_NAME);
        pds.add(TABLE_DEPENDENCIES);
        pds.add(EXCLUDE_TABLES);
        pds.add(LIMIT_COUNTER);
        pds.add(SPECIAL_LIMIT_TABLES);
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

    // Incremental Listener detects if there are changes to the source database, it then sets up flow files
    // with meta data and sends them to where they need to go (i.e. structure changes need to implement their
    // changes before data can come in)
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();

        // This flow file comes from the first step in the process, which will gather a list of tables and their primary keys
        // so that the migrator and incrementor can be multithreaded
        FlowFile fileToProcess = null;

        final ComponentLog logger = getLogger();
        // final StopWatch stopWatch = new StopWatch(true);
        final ProcessSession resultsSession = sessionFactory.createSession();

        // final Map<String, String> flowfile_log = new HashMap<>();
        final DBCPService dbcpIncomingService = context.getProperty(DBCP_SOURCE_DB).asControllerService(DBCPService.class);
        final DBCPService dbcpOutgoingService = context.getProperty(DBCP_TARGET_DB).asControllerService(DBCPService.class);

        String src_database_engine = context.getProperty(SOURCE_DATABASE_ENGINE).getValue();
        String src_database = context.getProperty(SOURCE_DATABASE).getValue();
        String src_schema = context.getProperty(SOURCE_SCHEMA).getValue();

        String tar_database_engine = context.getProperty(TARGET_DATABASE_ENGINE).getValue();
        String tar_database = context.getProperty(TARGET_DATABASE).getValue();
        String tar_schema = context.getProperty(TARGET_SCHEMA).getValue();

        // String table_dependencies = context.getProperty(TABLE_DEPENDENCIES).getValue();
        String exclude_tables_str = context.getProperty(EXCLUDE_TABLES).getValue();
        String limit = context.getProperty(LIMIT_COUNTER).getValue();
        String special_limit_tables = context.getProperty(SPECIAL_LIMIT_TABLES).getValue(); // If we have an incoming flow file, this will not be used

        HashSet<String> SQLKeywords = new HashSet<String>(Arrays.asList(new String[]{
                "ALL", "ALTER", "AND", "ANY", "ARRAY", "ARROW", "AS", "ASC", "AT",
                "BEGIN", "BETWEEN", "BY",
                "CASE", "CHECK", "CLUSTERS", "CLUSTER", "COLAUTH", "COLUMNS", "COMPRESS", "CONNECT", "CRASH", "CREATE", "CURRENT",
                "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP",
                "ELSE", "END", "EXCEPTION", "EXCLUSIVE", "EXISTS",
                "FETCH", "FORM", "FOR", "FROM",
                "GOTO", "GRANT", "GROUP",
                "HAVING",
                "IDENTIFIED", "IF", "IN", "INDEXES", "INDEX", "INSERT", "INTERSECT", "INTO", "IS",
                "LIKE", "LOCK",
                "MINUS", "MODE",
                "NOCOMPRESS", "NOT", "NOWAIT", "NULL",
                "OF", "ON", "OPTION", "OR", "ORDER", "OVERLAPS",
                "PRIOR", "PROCEDURE", "PUBLIC",
                "RANGE", "RECORD", "RESOURCE", "REVOKE",
                "SELECT", "SHARE", "SIZE", "SQL", "START", "SUBTYPE",
                "TABAUTH", "TABLE", "THEN", "TO", "TYPE",
                "UNION", "UNIQUE", "UPDATE", "USE",
                "VALUES", "VIEW", "VIEWS",
                "WHEN", "WHERE", "WITH",
                "A", "ADD", "AGENT", "AGGREGATE", "ARRAY", "ATTRIBUTE", "AUTHID", "AVG",
                "BFILE_BASE", "BINARY", "BLOB_BASE", "BLOCK", "BODY", "BOTH", "BOUND", "BULK", "BYTE",
                "C", "CALL", "CALLING", "CASCADE", "CHAR", "CHAR_BASE", "CHARACTER", "CHARSETFORM", "CHARSETID", "CHARSET", "CLOB_BASE", "CLOSE", "COLLECT", "COMMENT", "COMMIT", "COMMITTED",
                "COMPILED", "CONSTANT", "CONSTRUCTOR", "CONTEXT", "CONVERT", "COUNT", "CURSOR", "CUSTOMDATUM",
                "DANGLING", "DATA", "DATE", "DATE_BASE", "DAY", "DEFINE", "DETERMINISTIC", "DOUBLE", "DURATION",
                "ELEMENT", "ELSIF", "EMPTY", "ESCAPE", "EXCEPT", "EXCEPTIONS", "EXECUTE", "EXIT", "EXTERNAL",
                "FINAL", "FIXED", "FLOAT", "FORALL", "FORCE", "FUNCTION",
                "GENERAL",
                "HASH", "HEAP", "HIDDEN", "HOUR",
                "IMMEDIATE", "INCLUDING", "INDICATOR", "INDICES", "INFINITE", "INSTANTIABLE", "INT", "INTERFACE", "INTERVAL", "INVALIDATE", "ISOLATION",
                "JAVA",
                "LANGUAGE", "LARGE", "LEADING", "LENGTH", "LEVEL", "LIBRARY", "LIKE2", "LIKE4", "LIKEC", "LIMIT", "LIMITED", "LOCAL", "LONG", "LOOP",
                "MAP", "MAX", "MAXLEN", "MEMBER", "MERGE", "MIN", "MINUTE", "MOD", "MODIFY", "MONTH", "MULTISET",
                "NAME", "NAN", "NATIONAL", "NATIVE", "NCHAR", "NEW", "NOCOPY", "NUMBER_BASE",
                "OBJECT", "OCICOLL", "OCIDATETIME", "OCIDATE", "OCIDURATION", "OCIINTERVAL", "OCILOBLOCATOR", "OCINUMBER", "OCIRAW", "OCIREFCURSOR", "OCIREF", "OCIROWID", "OCISTRING",
                "OCITYPE", "ONLY", "OPAQUE", "OPEN", "OPERATOR", "ORACLE", "ORADATA", "ORGANIZATION", "ORLANY", "ORLVARY", "OTHERS", "OUT", "OVERRIDING",
                "PACKAGE", "PARALLEL_ENABLE", "PARAMETER", "PARAMETERS", "PARTITION", "PASCAL", "PIPE", "PIPELINED", "PRAGMA", "PRECISION", "PRIVATE",
                "RAISE", "RANGE", "RAW", "READ", "RECORD", "REF", "REFERENCE", "REM", "REMAINDER", "RENAME", "RESULT", "RETURN", "RETURNING", "REVERSE", "ROLLBACK", "ROW",
                "SAMPLE", "SAVE", "SAVEPOINT", "SB1", "SB2", "SB4", "SECOND", "SEGMENT", "SELF", "SEPARATE", "SEQUENCE", "SERIALIZABLE", "SET", "SHORT", "SIZE_T", "SOME", "SPARSE", "SQLCODE",
                "SQLDATA", "SQLNAME", "SQLSTATE", "STANDARD", "STATIC", "STDDEV", "STORED", "STRING", "STRUCT", "STYLE", "SUBMULTISET", "SUBPARTITION", "SUBSTITUTABLE", "SUBTYPE", "SUM", "SYNONYM",
                "TDO", "THE", "TIME", "TIMESTAMP", "TIMEZONE_ABBR", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TIMEZONE_REGION", "TRAILING", "TRANSAC", "TRANSACTIONAL", "TRUSTED", "TYPE",
                "UB1", "UB2", "UB4", "UNDER", "UNSIGNED", "UNTRUSTED", "USE", "USING",
                "VALIST", "VALUE", "VARIABLE", "VARIANCE", "VARRAY", "VARYING", "VOID",
                "WHILE", "WORK", "WRAPPED", "WRITE",
                "YEAR",
                "ZONE"
        }));

        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            if (fileToProcess == null && context.hasNonLoopConnection()) {
                session.transfer(fileToProcess, FAILURE);
                logger.error("No flow file found, throwing error.");
                context.yield();
            }
        }

        Map<String, Integer> table_record_count = new HashMap<>(); // This goes into flow files
        Map<String, String> table_max_date = new HashMap<>(); // This is used locally, it is not necessary to put into flow files
        Map<String, String> table_primary_keys = new HashMap<>(); // This goes into flow files
        Map<String, String> f_attr = new HashMap<>();

        Connection src_conn = null;
        Connection tar_conn = null;

        if (fileToProcess == null) {
            // Get list of tables and row counts for each table
            List<String> exclude_tables = new ArrayList<>();
            String tables_record_count_sql = "";
            String tables_primary_keys_sql = "";
            if (exclude_tables_str != null && !exclude_tables_str.isEmpty()) {
                exclude_tables = Arrays.asList(exclude_tables_str.split(","));
                tables_record_count_sql = "USE [" + src_database + "]; SELECT QUOTENAME(sOBJ.name) AS [table_name], SUM(sPTN.Rows) AS [row_count] FROM "
                        + "sys.objects AS sOBJ INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id WHERE sOBJ.type = 'U' "
                        + "AND sOBJ.is_ms_shipped = 0x0 AND index_id < 2 AND SCHEMA_NAME(sOBJ.schema_id) = '" + src_schema + "' AND sOBJ.name NOT IN ('" + exclude_tables_str + "') "
                        + "GROUP BY sOBJ.schema_id, sOBJ.name ORDER BY [table_name]";
                tables_primary_keys_sql = "SELECT T.[name] AS [table_name], C.[name] AS [column_name] "
                        + "FROM sys.[indexes] AS IND "
                        + "INNER JOIN sys.[index_columns] AS IC ON IND.[object_id]=IC.[object_id] AND IND.[index_id]=IC.[index_id] "
                        + "INNER JOIN sys.[columns] AS C ON IC.[object_id]=C.[object_id] AND IC.[column_id]=C.[column_id] "
                        + "INNER JOIN sys.[tables] AS T ON IND.[object_id]=T.[object_id] WHERE OBJECT_SCHEMA_NAME(T.[object_id],DB_ID())='dbo' "
                        + "AND IND.[is_primary_key] = 'true' ORDER BY T.[name] AND T.[name] NOT IN ('" + exclude_tables_str + "')";
            } else {
                tables_record_count_sql = "USE [" + src_database + "]; SELECT QUOTENAME(sOBJ.name) AS [table_name], SUM(sPTN.Rows) AS [row_count] FROM "
                        + "sys.objects AS sOBJ INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id WHERE sOBJ.type = 'U' "
                        + "AND sOBJ.is_ms_shipped = 0x0 AND index_id < 2 AND SCHEMA_NAME(sOBJ.schema_id) = '" + src_schema + "' "
                        + "GROUP BY sOBJ.schema_id, sOBJ.name ORDER BY [table_name]";
                tables_primary_keys_sql = "SELECT T.[name] AS [table_name], C.[name] AS [column_name] "
                        + "FROM sys.[indexes] AS IND "
                        + "INNER JOIN sys.[index_columns] AS IC ON IND.[object_id]=IC.[object_id] AND IND.[index_id]=IC.[index_id] "
                        + "INNER JOIN sys.[columns] AS C ON IC.[object_id]=C.[object_id] AND IC.[column_id]=C.[column_id] "
                        + "INNER JOIN sys.[tables] AS T ON IND.[object_id]=T.[object_id] WHERE OBJECT_SCHEMA_NAME(T.[object_id],DB_ID())='dbo' "
                        + "AND IND.[is_primary_key] = 'true' ORDER BY T.[name]";
            }
            String backup_date = this.getLatestSchemaDate(context, session);
            String struc_table_name = this.checkStructure(context, session, backup_date);
            if (!struc_table_name.isEmpty() && !struc_table_name.equals(null)) {
                // stop processing right here because we need to generate schema data
                fileToProcess = this.generateSchemaInformation(context, session, null, struc_table_name);
                session.transfer(fileToProcess, STRUCTURE);
                session.commit();
                context.yield();
            }
            try {
                src_conn = dbcpIncomingService.getConnection();
                tar_conn = dbcpOutgoingService.getConnection();
                Statement src_stat = src_conn.createStatement();
                ResultSet t_rec_count = src_stat.executeQuery(tables_record_count_sql);
                while (t_rec_count.next()) {
                    String table_name = t_rec_count.getString("table_name");
                    Integer ind_first = table_name.indexOf("[");
                    Integer ind_last = table_name.lastIndexOf("]");
                    table_name = table_name.substring(ind_first + 1, ind_last);
                    String temp_table_name = table_name;
                    if (SQLKeywords.contains(table_name)) {
                        temp_table_name = "`" + table_name + "`";
                    }
                    Integer row_count = t_rec_count.getInt("row_count");
                    table_record_count.put(table_name, row_count);
                    if (row_count > 100) {
                        String latest_date_sql = "";
                        latest_date_sql = "SELECT MAX( AuditEffectiveDateTime ) latest_date FROM " + temp_table_name + ";"; // Run this against target
                        Statement tar_stat = tar_conn.createStatement();
                        tar_stat.executeQuery("USE " + tar_database + ";");
                        ResultSet t_latest = tar_stat.executeQuery(latest_date_sql);
                        while (t_latest.next()) {
                            String temp_date = t_latest.getString("latest_date");
                            if (temp_date != null && !temp_date.isEmpty()) {
                                Integer index_period = temp_date.indexOf(".");
                                temp_date = temp_date.substring(0, index_period);
                            }
                            table_max_date.put(table_name, temp_date);
                        }
                        t_latest.close();
                        tar_stat.close();
                    }
                }
                t_rec_count.close();
                ResultSet t_primary_key = src_stat.executeQuery(tables_primary_keys_sql);
                while (t_primary_key.next()) {
                    String table_name = t_primary_key.getString("table_name");
                    String column_name = t_primary_key.getString("column_name");
                    table_primary_keys.put(table_name, column_name);
                }
                t_primary_key.close();
                src_stat.close();
            } catch (final ProcessException | SQLException e) {
                logger.error("Unable to execute SQL count query {} due to {}; routing to failure",
                        new Object[]{tables_record_count_sql, e});
                context.yield();
                throw new ProcessException("Unable to communicate with target database", e);
            }
        } else {
            String backup_date = this.getLatestSchemaDate(context, session);
            String struc_table_name = this.checkStructure(context, session, backup_date);
            if (!struc_table_name.isEmpty() && !struc_table_name.equals(null)) {
                // stop processing right here because we need to generate schema data
                fileToProcess = this.generateSchemaInformation(context, session, fileToProcess, struc_table_name);
                session.transfer(fileToProcess, STRUCTURE);
                session.commit();
                context.yield();
            }

            f_attr = fileToProcess.getAttributes();
//            src_database_engine = f_attr.get("source_database_engine"); // This will be used in the future
            src_database = f_attr.get("source_database");
            src_schema = f_attr.get("source_schema");
            String table_name = f_attr.get("source_table_name");
            String primary_key = f_attr.get("source_primary_key");

//            tar_database_engine = f_attr.get("target_database_engine"); // This will be used in the future
            tar_database = f_attr.get("target_database");
            tar_schema = f_attr.get("target_schema");

            limit = f_attr.get("limit_counter");

            String tables_record_count_sql = "USE [" + src_database + "]; SELECT QUOTENAME(sOBJ.name) AS [table_name], SUM(sPTN.Rows) AS [row_count] FRlOM "
                    + "sys.objects AS sOBJ INNER JOIN sys.partitions AS sPTN ON sOBJ.object_id = sPTN.object_id WHERE sOBJ.type = 'U' "
                    + "AND sOBJ.is_ms_shipped = 0x0 AND index_id < 2 AND SCHEMA_NAME(sOBJ.schema_id) = '" + src_schema + "' AND QUOTENAME(sOBJ.name) = '[" + table_name + "]' "
                    + "GROUP BY sOBJ.schema_id, sOBJ.name ORDER BY [table_name]";

            table_primary_keys.put(table_name, primary_key);

            session.remove(fileToProcess);

            try {
                src_conn = dbcpIncomingService.getConnection();
                tar_conn = dbcpOutgoingService.getConnection();
                Statement src_stat = src_conn.createStatement();
                ResultSet t_rec_count = src_stat.executeQuery(tables_record_count_sql);
                while (t_rec_count.next()) {
                    Integer row_count = t_rec_count.getInt("row_count");
                    table_record_count.put(table_name, row_count);
                    if (row_count > 100) {
                        String latest_date_sql = "";
                        String temp_table_name = table_name;
                        if (SQLKeywords.contains(table_name)) {
                            temp_table_name = "`" + table_name + "`";
                        }
                        latest_date_sql = "SELECT MAX( AuditEffectiveDateTime ) latest_date FROM " + temp_table_name + ";"; // Run this against target
                        Statement tar_stat = tar_conn.createStatement();
                        tar_stat.executeQuery("USE " + tar_database + ";");
                        ResultSet t_latest = tar_stat.executeQuery(latest_date_sql);
                        while (t_latest.next()) {
                            String temp_date = t_latest.getString("latest_date");
                            if (temp_date != null && !temp_date.isEmpty()) {
                                Integer index_period = temp_date.indexOf(".");
                                temp_date = temp_date.substring(0, index_period);
                            }
                            table_max_date.put(table_name, temp_date);
                        }
                        t_latest.close();
                        tar_stat.close();
                    }
                }
                t_rec_count.close();
            } catch (final ProcessException | SQLException e) {
                logger.error("Unable to execute SQL count query {} due to {}; routing to failure",
                        new Object[]{tables_record_count_sql, e});
                context.yield();
                throw new ProcessException("Unable to communicate with target database", e);
            }
        }

        // Check for table changes
        // For any table under 100 records, just go through row by row and check them.
        // For any table over 100 records, look for AuditEffectiveDateTime and use that column to get updates
        // SELECT COUNT(*) AS row_count FROM ClinicalDocument WHERE AuditEffectiveDateTime > CONVERT(datetime, '2016-05-10 00:00:00');
        for (Entry<String, Integer> entry : table_record_count.entrySet()) {
            System.gc(); // Making sure this runs as clean as possible, given that it will deal with a large amount of data
            Map<String, Map<String, List<String>>> table_tar_ids = new HashMap<>(); // this goes into flow files
            String table_name = entry.getKey().toString();
            Integer record_count = Integer.parseInt(entry.getValue().toString());
            // NOTE: record_count is the total number of records in the source table
            if (record_count <= 100 && record_count > 0) {
                // Truncate the table and insert all the records again.
                String tar_query = "USE " + tar_database + "; TRUNCATE " + table_name + ";";
                String src_query = "SELECT " + table_primary_keys.get(table_name) + " FROM " + table_name + ";";
                try {
                    src_conn = dbcpIncomingService.getConnection();
                    Statement src_stat = src_conn.createStatement();
                    tar_conn = dbcpOutgoingService.getConnection();
                    Statement tar_stat = tar_conn.createStatement();
                    tar_stat.executeQuery(tar_query);
                    ResultSet src_get_all = src_stat.executeQuery(src_query);
                    List<String> id_collector = new ArrayList<>();
                    while (src_get_all.next()) {
                        id_collector.add(src_get_all.getString(table_primary_keys.get(table_name)));
                    }
                    table_tar_ids.put(table_name, new HashMap<>());
                    table_tar_ids.get(table_name).put("insert", id_collector);
                    table_tar_ids.get(table_name).put("update", new ArrayList<>());
                    src_get_all.close();
                    src_stat.close();
                } catch (final ProcessException | SQLException e) {
                    logger.error("Unable to execute SQL count query {} due to {}; routing to failure",
                            new Object[]{tar_query, e});
                    context.yield();
                }
            } else {
                String table_rec_count_sql = "";
                String table_ids_src_sql = "";
                table_ids_src_sql = "USE [" + src_database + "]; SELECT " + table_primary_keys.get(table_name) + " FROM [" + table_name + "] "
                        + "WHERE AuditEffectiveDateTime > CONVERT(datetime, '" + table_max_date.get(table_name) + "') "
                        + "ORDER BY " + table_primary_keys.get(table_name) + " ASC;";
                // First lets see what's changed / what's new since the last pull, we do this by counting all records above our target date
                table_rec_count_sql = "USE [" + src_database + "]; SELECT COUNT(*) AS row_count FROM " + table_name + " WHERE AuditEffectiveDateTime > CONVERT(datetime, '"
                        + table_max_date.get(table_name) + "');";
                /* If count is greater than limit, then start up a job with the limit counter
                 * If count is less than limit, then stick all of the inserts into one insert statement
                 * If count is zero, nothing has changed, no need to continue further */
                try {
                    src_conn = dbcpIncomingService.getConnection();
                    tar_conn = dbcpOutgoingService.getConnection();
                    Statement src_stat = src_conn.createStatement();
                    Statement tar_stat = tar_conn.createStatement();
                    ResultSet src_count = src_stat.executeQuery(table_rec_count_sql);
                    int row_count = 0;
                    while (src_count.next()) {
                        row_count = Integer.parseInt(src_count.getString("row_count"));
                    }
                    src_count.close();

                    /* Run query against target to get complete list of IDs
                     * Run query against source to get IDs of recently changed items
                     * Compare list from source against target list, anything that matches up goes to update queue, all else to insert queue
                     * Insert queue simply chunks records together for mass insertion, but update will have to run individual queries */
                    if (row_count > 0) {
                        // Inserts will be configured with the limit in mind, Updates will be appended to a batch and run as singular statements
                        ResultSet src_ids = src_stat.executeQuery(table_ids_src_sql);
                        List<String> id_collector = new ArrayList<>();
                        String id_arrys = "";
                        while (src_ids.next()) {
                            id_collector.add(src_ids.getString(table_primary_keys.get(table_name)));
                            id_arrys = id_arrys + src_ids.getString(table_primary_keys.get(table_name)) + ",";
                        }
                        id_arrys = id_arrys.substring(0, id_arrys.length() - 1);
                        src_ids.close();
                        tar_stat.executeQuery("USE " + tar_database + ";");
                        String table_ids_tar_sql = "SELECT " + table_primary_keys.get(table_name) + " FROM " + table_name + " WHERE " + table_primary_keys.get(table_name)
                                + " IN (" + id_arrys + ") ORDER BY " + table_primary_keys.get(table_name) + " ASC;";
                        ResultSet tar_ids = tar_stat.executeQuery(table_ids_tar_sql); // Use SQL to get the list of IDs to update (WHERE IN syntax)
                        List<String> updates = new ArrayList<>();
                        while (tar_ids.next()) {
                            updates.add(tar_ids.getString(table_primary_keys.get(table_name)));
                        }
                        id_collector.removeAll(updates);
                        table_tar_ids.put(table_name, new HashMap<String, List<String>>());
                        table_tar_ids.get(table_name).put("insert", id_collector); // Records with ids in this map are going to generate insert statements
                        table_tar_ids.get(table_name).put("update", updates); // Records with ids in this map are going to generate update statements
                        tar_ids.close();
                    } else {
                        // If zero we're done with this record.
                        FlowFile file = resultsSession.create();
                        if (fileToProcess == null) {
                            file = resultsSession.putAttribute(file, "source_database_engine", src_database_engine);
                            file = resultsSession.putAttribute(file, "source_database", src_database);
                            file = resultsSession.putAttribute(file, "source_schema", src_schema);
                            file = resultsSession.putAttribute(file, "target_database_engine", tar_database_engine);
                            file = resultsSession.putAttribute(file, "target_database", tar_database);
                            file = resultsSession.putAttribute(file, "target_schema", tar_schema);
                            file = resultsSession.putAttribute(file, "source_table_name", table_name);
                        } else {
                            file = resultsSession.putAllAttributes(file, f_attr);
                        }
                        resultsSession.transfer(file, SUCCESS);
                        src_stat.close();
                        tar_stat.close();
                        continue;
                    }
                    src_stat.close();
                    tar_stat.close();
                } catch (final ProcessException | SQLException e) {
                    logger.error("Unable to execute SQL count query {} due to {}; routing to failure",
                            new Object[]{table_rec_count_sql, e});
                    context.yield();
                }
            }
            // Now it's time to setup all the flow files and send them on their way
            /* Foreach table */
            table_name = entry.getKey().toString();
            record_count = Integer.parseInt(entry.getValue().toString());
            String primary_key = table_primary_keys.get(table_name);
            String tar_update_ids_str = "";
            try {
                tar_update_ids_str = StringUtils.join(table_tar_ids.get(table_name).get("update"), ',');
            } catch (Exception e) {
                tar_update_ids_str = "";
            }
            String tar_insert_ids_str = "";
            try {
                tar_insert_ids_str = StringUtils.join(table_tar_ids.get(table_name).get("insert"), ',');
            } catch (Exception e) {
                tar_insert_ids_str = "";
            }

            List<String> special_limits = new ArrayList<>();
            try {
                special_limits = Arrays.asList(special_limit_tables.split("\\|"));
            } catch (Exception e) {
                special_limits = new ArrayList<>();
            }
            // We need to create a flow file for updates and inserts
            FlowFile file = resultsSession.create();
            if (!tar_update_ids_str.isEmpty() && !tar_update_ids_str.equals(null)) {
                file = resultsSession.putAttribute(file, "limit_counter", String.valueOf(limit));
                file = resultsSession.putAttribute(file, "sql_query_type", "update");
                file = resultsSession.putAttribute(file, "sql_query_call", "incrementor");
                file = resultsSession.putAttribute(file, "source_database_engine", "mssql");
                file = resultsSession.putAttribute(file, "source_database", src_database);
                file = resultsSession.putAttribute(file, "source_schema", src_schema);
                file = resultsSession.putAttribute(file, "source_table_name", table_name);
                file = resultsSession.putAttribute(file, "source_record_count", String.valueOf(record_count));
                file = resultsSession.putAttribute(file, "source_primary_key", primary_key);
                file = resultsSession.putAttribute(file, "source_id_string", tar_update_ids_str);
                file = resultsSession.putAttribute(file, "target_database_engine", "mariadb");
                file = resultsSession.putAttribute(file, "target_database", tar_database);
                file = resultsSession.putAttribute(file, "target_schema", tar_schema);
                file = resultsSession.putAttribute(file, "target_table_name", table_name);

                resultsSession.transfer(file, DATA);
            }

            if (!tar_insert_ids_str.isEmpty() && !tar_insert_ids_str.equals(null)) {
                // Insert flowfile
                Integer new_limit = new Integer(limit);
                if (!special_limits.isEmpty() && !special_limits.equals(null)) {
                    for (String sp_limit_table : special_limits) {
                        List<String> table_limit = Arrays.asList(sp_limit_table.split(","));
                        if (table_name.equals(table_limit.get(0))) {
                            new_limit = new Integer(Integer.parseInt(table_limit.get(1)));
                        } else {
                            continue;
                        }
                    }
                }

                if (!new_limit.equals(limit)) {
                    file = resultsSession.putAttribute(file, "limit_counter", String.valueOf(new_limit));
                } else {
                    file = resultsSession.putAttribute(file, "limit_counter", String.valueOf(limit));
                }
                file = resultsSession.putAttribute(file, "sql_query_type", "insert");
                file = resultsSession.putAttribute(file, "sql_query_call", "incrementor");
                file = resultsSession.putAttribute(file, "source_database_engine", "mssql");
                file = resultsSession.putAttribute(file, "source_database", src_database);
                file = resultsSession.putAttribute(file, "source_schema", src_schema);
                file = resultsSession.putAttribute(file, "source_table_name", table_name);
                file = resultsSession.putAttribute(file, "source_record_count", String.valueOf(record_count));
                file = resultsSession.putAttribute(file, "source_primary_key", primary_key);
                file = resultsSession.putAttribute(file, "source_id_string", tar_insert_ids_str);
                file = resultsSession.putAttribute(file, "target_database_engine", "mariadb");
                file = resultsSession.putAttribute(file, "target_database", tar_database);
                file = resultsSession.putAttribute(file, "target_schema", tar_schema);
                file = resultsSession.putAttribute(file, "target_table_name", table_name);

                resultsSession.transfer(file, DATA);
            }
            resultsSession.commit();
        }
    }

    private String getLatestSchemaDate(ProcessContext context, ProcessSession session) {
        // This piece will be completed later, but we need initial logic to tell the processor to go into this.
        // Run against target to determine when we last updated the schema
        String date = "";
        return date;
    }

    private String checkStructure(ProcessContext context, ProcessSession session, String date) {
        // This piece will be completed later, but we need initial logic to tell the processor to go into this.
        //        FlowFile file = session.create();
        //        final ProcessorLog logger = getLogger();
        //        final DBCPService dbcpIncomingService = context.getProperty(DBCP_SOURCE_DB).asControllerService(DBCPService.class);
        //        // Check for structural changes first (Send to another function so we can break out of the try block when necessary)
        //        Connection conn = null;
        //        String check_struc_changes_SQL = "SELECT [table_name] = name, modify_date FROM sys.tables WHERE modify_date > CONVERT(datetime, '"+date+"');";
        //        try
        //        {
        //            conn = dbcpIncomingService.getConnection();
        //            Statement stat = conn.createStatement();
        //            ResultSet struc_changes = stat.executeQuery(check_struc_changes_SQL);
        //            // Ideally there aren't any structure changes, so this query returning nothing is a good thing.
        //            if (!struc_changes.next())
        //            {
        //                // Break out of this function
        //                return true;
        //            }
        //            else
        //            {
        //                /* Create SQL to update structure, execute and then return in order to correct the structure,
        //                 * then perform everything that the gatherMetaData function does so that we can attach the migration
        //                 * platform to the structure changes. */
        //
        //                session.transfer(file, STRUCTURE);
        //                return false;
        //            }
        //        }
        //        catch ( final ProcessException | SQLException e)
        //        {
        //            logger.error("Unable to execute SQL count query {} due to {}; routing to failure",
        //                    new Object[]{check_struc_changes_SQL, e});
        //            context.yield();
        //        }
        String table_name = "";
        return table_name;
    }

    private FlowFile generateSchemaInformation(ProcessContext context, ProcessSession session, FlowFile file, String table_name) {
        // This gathers information about the schema for a specific table
        return file;
    }
}
