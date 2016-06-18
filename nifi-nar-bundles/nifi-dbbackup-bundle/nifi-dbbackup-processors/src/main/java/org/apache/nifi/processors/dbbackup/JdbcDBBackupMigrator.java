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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.nifi.annotation.behavior.EventDriven;
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
import org.apache.nifi.util.StopWatch;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "insert", "jdbc", "query", "database", "xml"})
@CapabilityDescription("This processor connects to and gathers metadata information from source and target databases.")
@Stateful(scopes = {Scope.LOCAL}, description = "Database fields can be saved as state info using the 'State Fields' property")
@WritesAttribute(attribute = "¿any?", description = "This processor may write zero or more attributes")
public class JdbcDBBackupMigrator extends AbstractSessionFactoryProcessor {
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

    private static final Set<Relationship> relationships;

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

    public static final PropertyDescriptor SOURCE_DATABASE_ENGINE = new PropertyDescriptor.Builder()
            .name("Source Database Engine")
            .description("The database engine that source is running.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SOURCE_DATABASE = new PropertyDescriptor.Builder()
            .name("Source Database Name")
            .description("The name of the database to be copied.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SOURCE_SCHEMA = new PropertyDescriptor.Builder()
            .name("Source Schema Name")
            .description("The name of the database schema to be copied (MySQL doesn't necessarily use this).")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    public static final PropertyDescriptor TARGET_DATABASE_ENGINE = new PropertyDescriptor.Builder()
            .name("Target Database Engine")
            .description("The database engine that target is running.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TARGET_DATABASE = new PropertyDescriptor.Builder()
            .name("Target Database Name")
            .description("The name of the database we're copying to.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TARGET_SCHEMA = new PropertyDescriptor.Builder()
            .name("Target Schema Name")
            .description("The name of the database schema we're copying to.")
            .addValidator(Validator.VALID)
            .required(false)
            .build();

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

    public static final PropertyDescriptor SCHEMA_MANUALLY_GENERATED = new PropertyDescriptor.Builder()
            .name("Schema Manually Generated")
            .description("If true, the processor will only check for changes to the schema and apply any changes it finds.  If false, processor will assume it needs to generate "
                    + "the schema and gather information accordingly.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final List<PropertyDescriptor> propDescriptors;

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
        pds.add(TABLE_DEPENDENCIES);
        pds.add(EXCLUDE_TABLES);
        pds.add(LIMIT_COUNTER);
        pds.add(SPECIAL_LIMIT_TABLES);
        pds.add(SCHEMA_MANUALLY_GENERATED);
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

    // TODO Build out schema creation functionality

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();

        // This flow file comes from the first step in the process, which will gather a list of tables and their primary keys so that the migrator and incrementor can be multithreaded
        FlowFile fileToProcess = null;

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        final ProcessSession resultsSession = sessionFactory.createSession(); // If we have an incoming flow file, this will not be used

        Map<String, String> flowfile_log = new HashMap<>();
        final DBCPService dbcpIncomingService = context.getProperty(DBCP_SOURCE_DB).asControllerService(DBCPService.class);
        final DBCPService dbcpOutgoingService = context.getProperty(DBCP_TARGET_DB).asControllerService(DBCPService.class);

        String src_database_engine = context.getProperty(SOURCE_DATABASE_ENGINE).getValue(); // If we have an incoming flow file, this will not be used
        String src_database = context.getProperty(SOURCE_DATABASE).getValue(); // If we have an incoming flow file, this will not be used
        String src_schema = context.getProperty(SOURCE_SCHEMA).getValue(); // If we have an incoming flow file, this will not be used

        String tar_database_engine = context.getProperty(TARGET_DATABASE_ENGINE).getValue(); // If we have an incoming flow file, this will not be used
        String tar_database = context.getProperty(TARGET_DATABASE).getValue(); // If we have an incoming flow file, this will not be used
        String tar_schema = context.getProperty(TARGET_SCHEMA).getValue(); // If we have an incoming flow file, this will not be used

        String table_dependencies = context.getProperty(TABLE_DEPENDENCIES).getValue(); // If we have an incoming flow file, this will not be used
        /* table_dependencies should be formatted like so: table1,table2,table3|table1,table2,table3|etc...
         * The idea is that the tables listed within the section before the first pipe | should be run first, because these tables need to be filled with
         * data before other tables can be filled (db relationships).  Each corresponding pipe | designates that those tables should be run next. */
        String exclude_tables_str = context.getProperty(EXCLUDE_TABLES).getValue(); // If we have an incoming flow file, this will not be used
        /* exclude_tables_str should just be a comma separated list */
        String limit = context.getProperty(LIMIT_COUNTER).getValue(); // If we have an incoming flow file, this will not be used
        String special_limit_tables = context.getProperty(SPECIAL_LIMIT_TABLES).getValue(); // If we have an incoming flow file, this will not be used
        /* special_limit_tables designates tables that have larger data per record than other tables, therefore we can't run the same size chunks as the
         * rest of the tables.  The format for this variable is: table1,limit|table2,limit|etc,etc... */
        final boolean schema_generated = context.getProperty(SCHEMA_MANUALLY_GENERATED).asBoolean();

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
                "C", "CALL", "CALLING", "CASCADE", "CHAR", "CHAR_BASE", "CHARACTER", "CHARSETFORM", "CHARSETID", "CHARSET", "CLOB_BASE", "CLOSE", "COLLECT", "COMMENT", "COMMIT",
                "COMMITTED", "COMPILED", "CONSTANT", "CONSTRUCTOR", "CONTEXT", "CONVERT", "COUNT", "CURSOR", "CUSTOMDATUM",
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
                "OBJECT", "OCICOLL", "OCIDATETIME", "OCIDATE", "OCIDURATION", "OCIINTERVAL", "OCILOBLOCATOR", "OCINUMBER", "OCIRAW", "OCIREFCURSOR", "OCIREF", "OCIROWID",
                "OCISTRING", "OCITYPE", "ONLY", "OPAQUE", "OPEN", "OPERATOR", "ORACLE", "ORADATA", "ORGANIZATION", "ORLANY", "ORLVARY", "OTHERS", "OUT", "OVERRIDING",
                "PACKAGE", "PARALLEL_ENABLE", "PARAMETER", "PARAMETERS", "PARTITION", "PASCAL", "PIPE", "PIPELINED", "PRAGMA", "PRECISION", "PRIVATE",
                "RAISE", "RANGE", "RAW", "READ", "RECORD", "REF", "REFERENCE", "REM", "REMAINDER", "RENAME", "RESULT", "RETURN", "RETURNING", "REVERSE", "ROLLBACK", "ROW",
                "SAMPLE", "SAVE", "SAVEPOINT", "SB1", "SB2", "SB4", "SECOND", "SEGMENT", "SELF", "SEPARATE", "SEQUENCE", "SERIALIZABLE", "SET", "SHORT", "SIZE_T", "SOME", "SPARSE",
                "SQLCODE", "SQLDATA", "SQLNAME", "SQLSTATE", "STANDARD", "STATIC", "STDDEV", "STORED", "STRING", "STRUCT", "STYLE", "SUBMULTISET", "SUBPARTITION", "SUBSTITUTABLE", "SUBTYPE",
                "SUM", "SYNONYM", "TDO", "THE", "TIME", "TIMESTAMP", "TIMEZONE_ABBR", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TIMEZONE_REGION", "TRAILING", "TRANSAC", "TRANSACTIONAL",
                "TRUSTED", "TYPE", "UB1", "UB2", "UB4", "UNDER", "UNSIGNED", "UNTRUSTED", "USE", "USING",
                "VALIST", "VALUE", "VARIABLE", "VARIANCE", "VARRAY", "VARYING", "VOID",
                "WHILE", "WORK", "WRAPPED", "WRITE",
                "YEAR",
                "ZONE"
        }));

        Map<String, Integer> table_record_count = new HashMap<>(); // This goes into flow files
        Map<String, String> table_tar_record_count = new HashMap<>(); // This is used locally, it is not necessary to put into flow files
        Map<String, String> table_primary_keys = new HashMap<>(); // This goes into flow files

        Connection src_conn = null;
        Connection tar_conn = null;

        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            if (fileToProcess == null && context.hasNonLoopConnection()) {
                session.transfer(fileToProcess, FAILURE);
                logger.error("No flow file found, throwing error.");
                context.yield();
            }
        }

        if (!schema_generated) {
            // We're automatically assuming we need to generate the schema here.
            fileToProcess = this.generateSchemaInformation(context, session, null);
            session.transfer(fileToProcess, STRUCTURE);
            session.commit();
            context.yield();
        }

        if (fileToProcess == null) {
            // Go on without a flow file, in other words we don't have a processor before this, so it may take a little longer to run everything.
            List<String> exclude_tables = new ArrayList<>();
            String tables_record_count_sql = "";
            String tables_primary_keys_sql = "";
            /* Eventually these two sql strings will go into an SQL generator that takes the database engine into account. */
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
                        + "AND IND.[is_primary_key] = 'true' AND T.[name] NOT IN ('" + exclude_tables_str + "') ORDER BY T.[name]";
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
            /* Eventually all the SQL strings will go into a SQL generator */
            String backup_date = this.getLatestSchemaDate(context, session);
            if (!this.checkStructure(context, session, backup_date)) {
                // stop processing right here because we need to generate schema data
                fileToProcess = this.generateSchemaInformation(context, session, null);
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
                    Integer row_count = t_rec_count.getInt("row_count");
                    table_record_count.put(table_name, row_count);
                    if (row_count > 100) {
                        String latest_id_sql = "";
                        String temp_table_name = table_name;
                        if (SQLKeywords.contains(table_name.toUpperCase())) {
                            temp_table_name = "`" + table_name + "`";
                        }
                        /* Eventually these sql strings will go into an SQL generator that takes the database engine into account. */
                        // With this query we will eventually need to account for the fact that not all tables have AuditEffectiveDateTime in them
//                        tar_count_sql = "SELECT COUNT(*) AS record_count FROM "+table_name+";";
                        latest_id_sql = "SELECT COUNT(*) AS record_count FROM " + temp_table_name + ";";
                        /* Eventually all the SQL strings will go into a SQL generator */
                        Statement tar_stat = tar_conn.createStatement();
                        /* Eventually these sql strings will go into an SQL generator that takes the database engine into account. */
                        tar_stat.executeQuery("USE " + tar_database + ";");
                        /* Eventually all the SQL strings will go into a SQL generator */
                        ResultSet t_latest = tar_stat.executeQuery(latest_id_sql);
                        while (t_latest.next()) {
                            String temp_count = t_latest.getString("record_count");
                            table_tar_record_count.put(table_name, temp_count);
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
            if (!this.checkStructure(context, session, backup_date)) {
                // stop processing right here because we need to generate schema data
                fileToProcess = this.generateSchemaInformation(context, session, fileToProcess);
                session.transfer(fileToProcess, STRUCTURE);
                session.commit();
                context.yield();
            }
            // The flow file should have a table name and it's primary key, it should have already excluded tables, and it should only release certain tables to be
            // processed right now (table dependencies)
            Map<String, String> f_attr = fileToProcess.getAttributes();
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

            try {
                src_conn = dbcpIncomingService.getConnection();
                tar_conn = dbcpOutgoingService.getConnection();
                Statement src_stat = src_conn.createStatement();
                ResultSet t_rec_count = src_stat.executeQuery(tables_record_count_sql);
                while (t_rec_count.next()) {
                    Integer row_count = t_rec_count.getInt("row_count");
                    table_record_count.put(table_name, row_count);
                    if (row_count > 100) {
                        String latest_id_sql = "";
                        String temp_table_name = table_name;
                        if (SQLKeywords.contains(table_name)) {
                            temp_table_name = "`" + table_name + "`";
                        }
                        /* Eventually these sql strings will go into an SQL generator that takes the database engine into account. */
                        // With this query we will eventually need to account for the fact that not all tables have AuditEffectiveDateTime in them
                        latest_id_sql = "SELECT COUNT(*) AS record_count FROM " + temp_table_name + ";";
                        /* Eventually all the SQL strings will go into a SQL generator */
                        Statement tar_stat = tar_conn.createStatement();
                        /* Eventually these sql strings will go into an SQL generator that takes the database engine into account. */
                        tar_stat.executeQuery("USE " + tar_database + ";");
                        /* Eventually all the SQL strings will go into a SQL generator */
                        ResultSet t_latest = tar_stat.executeQuery(latest_id_sql);
                        while (t_latest.next()) {
                            String temp_count = t_latest.getString("record_count");
                            table_tar_record_count.put(table_name, temp_count);
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
        /* Take care of the special limit tables here, there's no reason to do it above */
        List<String> special_limits = new ArrayList<>();
        try {
            special_limits = Arrays.asList(special_limit_tables.split("\\|"));
        } catch (Exception e) {
            special_limits = new ArrayList<>();
        }
        /* table dependencies comes into play somewhere within this block, if there is not a parent processor feeding flow files into this processor */
        for (Entry<String, Integer> entry : table_record_count.entrySet()) {
            System.gc();
            String table_name = entry.getKey().toString();
            Integer record_count = Integer.parseInt(entry.getValue().toString());

            // Now it's time to setup all the flow files and send them on their way
            FlowFile file = null;
            try {
                file = resultsSession.create();
                if (fileToProcess == null) {
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
                    file = resultsSession.putAttribute(file, "sql_query_call", "migrator");
                    file = resultsSession.putAttribute(file, "source_database_engine", "mssql");
                    file = resultsSession.putAttribute(file, "source_database", src_database);
                    file = resultsSession.putAttribute(file, "source_schema", src_schema);
                    file = resultsSession.putAttribute(file, "source_table_name", table_name);
                    file = resultsSession.putAttribute(file, "source_record_count", String.valueOf(record_count));
                    file = resultsSession.putAttribute(file, "source_primary_key", table_primary_keys.get(table_name));
                    file = resultsSession.putAttribute(file, "target_record_count", table_tar_record_count.get(table_name));
                    file = resultsSession.putAttribute(file, "target_database_engine", "mariadb");
                    file = resultsSession.putAttribute(file, "target_database", tar_database);
                    file = resultsSession.putAttribute(file, "target_schema", tar_schema);
                    file = resultsSession.putAttribute(file, "target_table_name", table_name);
                } else {
                    Map<String, String> f_attr = fileToProcess.getAttributes();
                    file = resultsSession.putAllAttributes(file, f_attr);
                    file = resultsSession.putAttribute(file, "sql_query_type", "insert");
                    file = resultsSession.putAttribute(file, "sql_query_call", "migrator");
                    file = resultsSession.putAttribute(file, "source_record_count", String.valueOf(record_count));
                    file = resultsSession.putAttribute(file, "target_record_count", table_tar_record_count.get(table_name));
                    file = resultsSession.putAttribute(file, "target_table_name", table_name);
                }
                resultsSession.transfer(file, DATA);
            } catch (Exception e) {
                resultsSession.transfer(file, FAILURE);
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

    private boolean checkStructure(ProcessContext context, ProcessSession session, String date) {
        // This piece will be completed later, but we need initial logic to tell the processor to go into this.
        // FlowFile file = session.create();
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
        return true;
    }

    private FlowFile generateSchemaInformation(ProcessContext context, ProcessSession session, FlowFile file) {
        // This gathers information about the schema for either a specific table or all tables
        if (file == null) {
            // All tables
            file = session.create();
        } else {
            // Specific table
        }

        return file;
    }
}