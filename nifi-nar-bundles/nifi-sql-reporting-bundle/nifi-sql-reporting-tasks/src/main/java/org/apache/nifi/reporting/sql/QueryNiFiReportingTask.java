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
package org.apache.nifi.reporting.sql;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.sql.bulletins.BulletinTable;
import org.apache.nifi.reporting.sql.connectionstatus.ConnectionStatusTable;
import org.apache.nifi.reporting.sql.metrics.JvmMetricsTable;
import org.apache.nifi.reporting.sql.processgroupstatus.ProcessGroupStatusTable;
import org.apache.nifi.reporting.sql.processorstatus.ProcessorStatusTable;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Tags({"status", "connection", "processor", "jvm", "metrics", "history", "bulletin", "sql"}) // TODO
@CapabilityDescription("Publishes NiFi status information based on the results of a user-specified SQL query.")
public class QueryNiFiReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor RECORD_SINK = new PropertyDescriptor.Builder()
            .name("sql-reporting-record-sink")
            .displayName("Record Destination Service")
            .description("Specifies the Controller Service to use for writing out the records to some destination.")
            .identifiesControllerService(RecordSinkService.class)
            .required(true)
            .build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("sql-reporting-query")
            .displayName("SQL Query")
            .description("SQL SELECT statement specifies which tables to query and how data should be filtered/transformed. "
                    + "SQL SELECT can select from the CONNECTION_STATUS,PROCESSOR_STATUS,BULLETINS or JVM_METRICS tables") // TODO
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new SqlValidator())
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_RESULTS = new PropertyDescriptor.Builder()
            .name("sql-reporting-include-zero-record-results")
            .displayName("Include Zero Record Results")
            .description("When running the SQL statement, if the result has no data, "
                    + "this property specifies whether or not a Site-to-Site message (flow file, e.g.) will be transmitted.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;

    private volatile RecordSinkService recordSinkService;

    private final Cache<String, BlockingQueue<CachedStatement>> statementQueues = Caffeine.newBuilder()
            .maximumSize(25)
            .removalListener(this::onCacheEviction)
            .build();

    @Override
    protected void init(final ReportingInitializationContext config) throws InitializationException {
        try {
            DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        } catch (final SQLException e) {
            throw new ProcessException("Failed to load Calcite JDBC Driver", e);
        }

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QUERY);
        properties.add(RECORD_SINK);
        properties.add(INCLUDE_ZERO_RECORD_RESULTS);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final StopWatch stopWatch = new StopWatch(true);
        try {
            recordSinkService = context.getProperty(RECORD_SINK).asControllerService(RecordSinkService.class);
            final String sql = context.getProperty(QUERY).evaluateAttributeExpressions().getValue();
            if(sql == null) {

            }
            final QueryResult queryResult = query(context, sql);
            getLogger().debug("Executing query: {}", new Object[]{sql});

                final ResultSet rs = queryResult.getResultSet();
                final ResultSetRecordSet recordSet;
                // Create the RecordSchema from the ResultSet (same way ExecuteSQL does it)
                final RecordSchema writerSchema = AvroTypeUtil.createSchema(JdbcCommon.createSchema(rs));

                try {
                    recordSet = new ResultSetRecordSet(rs, writerSchema);
                } catch (final SQLException e) {
                    getLogger().error("Error creating record set from query results due to {}", new Object[]{e.getMessage()}, e);
                    return;
                }

            try {
                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());
                recordSinkService.sendData(recordSet, attributes, context.getProperty(INCLUDE_ZERO_RECORD_RESULTS).asBoolean());
            } catch (Exception e) {
                getLogger().error("Error during transmission of query results due to {}", new Object[]{e.getMessage()}, e);
                return;
            } finally {
                closeQuietly(queryResult);
            }
            final long elapsedMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            getLogger().debug("Successfully queried and sent in {} millis", new Object[]{elapsedMillis});
        } catch (Exception se) {
            throw new ProcessException(se);
        }
    }

    private synchronized CachedStatement getStatement(final String sql, final Supplier<CachedStatement> statementBuilder) {
        final BlockingQueue<CachedStatement> statementQueue = statementQueues.get(sql, key -> new LinkedBlockingQueue<>());

        final CachedStatement cachedStmt = statementQueue.poll();
        if (cachedStmt != null) {
            return cachedStmt;
        }

        return statementBuilder.get();
    }

    private CachedStatement buildCachedStatement(final String sql, final ReportingContext context) {

        final CalciteConnection connection = createConnection();
        final SchemaPlus rootSchema = createRootSchema(connection);

        final ConnectionStatusTable connectionStatusTable = new ConnectionStatusTable(context, getLogger());
        rootSchema.add("CONNECTION_STATUS", connectionStatusTable);
        final ProcessorStatusTable processorStatusTable = new ProcessorStatusTable(context, getLogger());
        rootSchema.add("PROCESSOR_STATUS", processorStatusTable);
        final ProcessGroupStatusTable processGroupStatusTable = new ProcessGroupStatusTable(context, getLogger());
        rootSchema.add("PROCESS_GROUP_STATUS", processGroupStatusTable);
        final JvmMetricsTable jvmMetricsTable = new JvmMetricsTable(context, getLogger());
        rootSchema.add("JVM_METRICS", jvmMetricsTable);
        final BulletinTable bulletinTable = new BulletinTable(context, getLogger());
        rootSchema.add("BULLETINS", bulletinTable);

        // TODO add the others
        rootSchema.setCacheEnabled(false);

        try {
            final PreparedStatement stmt = connection.prepareStatement(sql);
            return new CachedStatement(stmt, connection);
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    protected QueryResult query(final ReportingContext context, final String sql)
            throws SQLException {

        final Supplier<CachedStatement> statementBuilder = () -> buildCachedStatement(sql, context);

        final CachedStatement cachedStatement = getStatement(sql, statementBuilder);
        final PreparedStatement stmt = cachedStatement.getStatement();
        final ResultSet rs;
        try {
            rs = stmt.executeQuery();
        } catch (final Throwable t) {
            throw t;
        }

        return new QueryResult() {
            @Override
            public void close() throws IOException {
                final BlockingQueue<CachedStatement> statementQueue = statementQueues.getIfPresent(sql);
                if (statementQueue == null || !statementQueue.offer(cachedStatement)) {
                    try {
                        cachedStatement.getConnection().close();
                    } catch (SQLException e) {
                        throw new IOException("Failed to close statement", e);
                    }
                }
            }

            @Override
            public ResultSet getResultSet() {
                return rs;
            }

            @Override
            public int getRecordsRead() {
                return 0;
            }

        };
    }

    private SchemaPlus createRootSchema(final CalciteConnection calciteConnection) {
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        // Add any custom functions here
        return rootSchema;
    }

    private CalciteConnection createConnection() {
        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

        try {
            final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
            final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            return calciteConnection;
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    private void onCacheEviction(final String key, final BlockingQueue<CachedStatement> queue, final RemovalCause cause) {
        clearQueue(queue);
    }

    private void clearQueue(final BlockingQueue<CachedStatement> statementQueue) {
        CachedStatement stmt;
        while ((stmt = statementQueue.poll()) != null) {
            closeQuietly(stmt.getStatement(), stmt.getConnection());
        }
    }

    private void closeQuietly(final AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }

        for (final AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close SQL resource", e);
            }
        }
    }

    private static class SqlValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(true)
                        .explanation("Expression Language Present")
                        .build();
            }

            final String substituted = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();

            final SqlParser.Config config = SqlParser.configBuilder()
                    .setLex(Lex.MYSQL_ANSI)
                    .build();

            final SqlParser parser = SqlParser.create(substituted, config);
            try {
                parser.parseStmt();
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(true)
                        .build();
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Not a valid SQL Statement: " + e.getMessage())
                        .build();
            }
        }
    }

    private interface QueryResult extends Closeable {
        ResultSet getResultSet();
        int getRecordsRead();
    }

    private static class CachedStatement {
        private final PreparedStatement statement;
        private final Connection connection;

        CachedStatement(final PreparedStatement statement, final Connection connection) {
            this.statement = statement;
            this.connection = connection;
        }

        PreparedStatement getStatement() {
            return statement;
        }

        Connection getConnection() {
            return connection;
        }
    }
}
