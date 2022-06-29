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
package org.apache.nifi.processors.airtable;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
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

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;


@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"airtable", "query", "database"})
@CapabilityDescription("Generates an Airtable REST API query and executes it to fetch all rows whose values in the specified "
        + "Maximum Value column(s) are larger than the "
        + "previously-seen maxima. Query result will be output in JSON format. Expression Language is supported for several properties, but no incoming "
        + "connections are permitted. The Variable Registry may be used to provide values for any property containing Expression Language. "
        + "Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer or cron expression, using the standard scheduling methods. This processor is intended to be run on the Primary Node only. FlowFile attribute "
        + "'queryairtable.row.count' indicates how many rows were selected.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "tablename", description = "Name of the table being queried"),
        @WritesAttribute(attribute = "queryairtable.row.count", description = "The number of rows selected by the query"),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "maxvalue.*", description = "Each attribute contains the observed maximum value of a specified 'Maximum-value Column'. The "
                + "suffix of the attribute is the name of the column. If Output Batch Size is set, then this attribute will not be populated.")})
@DynamicProperty(name = "initial.maxvalue.<max_value_column>", value = "Initial maximum value for the specified column",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY, description = "Specifies an initial max value for max value column(s). Properties should "
        + "be added in the format `initial.maxvalue.<max_value_column>`. This value is only used the first time the table is accessed (when a Maximum Value Column is specified).")
@PrimaryNodeOnly
public class QueryAirtableTable extends AbstractSessionFactoryProcessor {

    public static final String RESULT_TABLENAME = "tablename";
    public static final String RESULT_ROW_COUNT = "querydbtable.row.count";
    public static final String INITIAL_MAX_VALUE_PROP_START = "initial.maxvalue.";

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("qat-page-size")
            .displayName("Page Size")
            .description("The number of result rows to be fetched from the API query at a time.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("qat-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("qat-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The maxvalue.* and fragment.count attributes will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    // Properties
    public static final PropertyDescriptor AIRTABLE_SERVICE = new PropertyDescriptor.Builder()
            .name("Airtable Service")
            .description("The Controller Service that is used to issue queries against Airtable.")
            .required(true)
            .identifiesControllerService(AirtableService.class)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the database table to be queried.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Columns to Return")
            .description("A comma-separated list of column names to be used in the query. If your database requires "
                    + "special treatment of the names (quoting, e.g.), each name should include such treatment. If no "
                    + "column names are supplied, all columns in the specified table will be returned. NOTE: It is important "
                    + "to use consistent column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Maximum-value Columns")
            .description("A comma-separated list of column names. The processor will keep track of the maximum value "
                    + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
                    + "to the column list, and each column's values are expected to increase more slowly than the previous columns' values. Thus, "
                    + "using multiple columns implies a hierarchical structure of columns, which is usually used for partitioning tables. This processor "
                    + "can be used to retrieve only those rows that have been added/updated since the last retrieval. Note that some "
                    + "JDBC types such as bit/boolean are not conducive to maintaining maximum value, so columns of these "
                    + "types should not be listed in this property, and will result in error(s) during processing. If no columns "
                    + "are provided, all rows from the table will be considered, which could have a performance impact. NOTE: It is important "
                    + "to use consistent max-value column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from Airatble API query results")
            .build();

    protected List<PropertyDescriptor> propDescriptors;
    protected Set<Relationship> relationships;

    // This value is cleared when the processor is scheduled, and set to true after setup() is called and completes successfully. This enables
    // the setup logic to be performed in onTrigger() versus OnScheduled to avoid any issues with DB connection when first scheduled to run.
    protected final AtomicBoolean setupComplete = new AtomicBoolean(false);

    // A Map (name to value) of initial maximum-value properties, filled at schedule-time and used at trigger-time
    protected Map<String, String> maxValueProperties;

    // The delimiter to use when referencing qualified names (such as table@!@column in the state map)
    protected static final String NAMESPACE_DELIMITER = "@!@";

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final boolean maxValueColumnNames = validationContext.getProperty(MAX_VALUE_COLUMN_NAMES).isSet();
        return results;
    }

    public QueryAirtableTable() {
        relationships = Set.of(REL_SUCCESS);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(AIRTABLE_SERVICE);
        pds.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(TABLE_NAME)
                .description("The name of the database table to be queried. When a custom query is used, this property is used to alias the query and appears as an attribute on the FlowFile.")
                .build());
        pds.add(COLUMN_NAMES);
        pds.add(MAX_VALUE_COLUMN_NAMES);
        pds.add(PAGE_SIZE);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(OUTPUT_BATCH_SIZE);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context, null);
    }

    @OnStopped
    public void stop() {
        // Reset the column type map in case properties change
        setupComplete.set(false);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final AirtableService airtableService = context.getProperty(AIRTABLE_SERVICE).asControllerService(AirtableService.class);
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final Integer pageSize = context.getProperty(PAGE_SIZE).evaluateAttributeExpressions().asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;

        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        //If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
        for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
            String maxPropKey = maxProp.getKey().toLowerCase();
            String fullyQualifiedMaxPropKey = getStateKey(tableName, maxPropKey);
            if (!statePropertyMap.containsKey(fullyQualifiedMaxPropKey)) {
                String newMaxPropValue;
                // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                // the value has been stored under a key that is only the column name. Fall back to check the column name,
                // but store the new initial max value under the fully-qualified key.
                if (statePropertyMap.containsKey(maxPropKey)) {
                    newMaxPropValue = statePropertyMap.get(maxPropKey);
                } else {
                    newMaxPropValue = maxProp.getValue();
                }
                statePropertyMap.put(fullyQualifiedMaxPropKey, newMaxPropValue);

            }
        }

        List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                ? null
                : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));

        final String selectQuery = getQuery(tableName, columnNames, maxValueColumnNameList, statePropertyMap);
        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();

        try (final Connection con = airtableService.getConnection(Collections.emptyMap());
             final Statement st = con.createStatement()) {

            if (pageSize != null && pageSize > 0) {
                try {
                    st.setFetchSize(pageSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{pageSize, se.getLocalizedMessage()}, se);
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Executing query {}", new Object[]{selectQuery});
            }
            try (final ResultSet resultSet = st.executeQuery(selectQuery)) {
                int fragmentIndex = 0;
                // Max values will be updated in the state property map by the callback
                final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap);

                while (true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    FlowFile fileToProcess = session.create();
                    try {
                        fileToProcess = session.write(fileToProcess, out -> {
                            try {
                                nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), maxValCollector));
                            } catch (Exception e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(fileToProcess);
                        throw e;
                    }

                    if (nrOfRows.get() > 0) {
                        // set attributes
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                        attributesToAdd.put(RESULT_TABLENAME, tableName);

                        attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                        fileToProcess = session.putAllAttributes(fileToProcess, attributesToAdd);
                        sqlWriter.updateCounters(session);

                        logger.debug("{} contains {} records; transferring to 'success'",
                                new Object[]{fileToProcess, nrOfRows.get()});

                        session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);
                        // If we've reached the batch size, send out the flow files
                        if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commitAsync();
                            resultSetFlowFiles.clear();
                        }
                    } else {
                        // If there were no rows returned, don't send the flowfile
                        session.remove(fileToProcess);
                        // If no rows and this was first FlowFile, yield
                        if (fragmentIndex == 0) {
                            context.yield();
                        }
                        break;
                    }

                    // If we aren't splitting up the data into flow files or fragments, then the result set has been entirely fetched so don't loop back around
                    if (maxRowsPerFlowFile == 0) {
                        break;
                    }

                    // If we are splitting up the data into flow files, don't loop back around if we've gotten all results
                    if (maxRowsPerFlowFile > 0 && nrOfRows.get() < maxRowsPerFlowFile) {
                        break;
                    }
                }

                // Apply state changes from the Max Value tracker
                maxValCollector.applyStateChanges();

                // Even though the maximum value and total count are known at this point, to maintain consistent behavior if Output Batch Size is set, do not store the attributes
                if (outputBatchSize == 0) {
                    for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                        // Add maximum values as attributes
                        for (Map.Entry<String, String> entry : statePropertyMap.entrySet()) {
                            // Get just the column name from the key
                            String key = entry.getKey();
                            String colName = key.substring(key.lastIndexOf(NAMESPACE_DELIMITER) + NAMESPACE_DELIMITER.length());
                            resultSetFlowFiles.set(i, session.putAttribute(resultSetFlowFiles.get(i), "maxvalue." + colName, entry.getValue()));
                        }

                        //set count on all FlowFiles
                        if (maxRowsPerFlowFile > 0) {
                            resultSetFlowFiles.set(i,
                                    session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                        }
                    }
                }
            } catch (final SQLException e) {
                throw e;
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", selectQuery, e);
            if (!resultSetFlowFiles.isEmpty()) {
                session.remove(resultSetFlowFiles);
            }
            context.yield();
        } finally {
            try {
                // Update the state
                session.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", this, ioe);
            }

            session.commitAsync();
        }
    }
    
    protected String getQuery(String tableName, String columnNames, List<String> maxValColumnNames, Map<String, String> stateMap) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name must be specified");
        }
        final StringBuilder query;

        query = new StringBuilder(/* TODO dbAdapter.getSelectStatement(tableName, columnNames, null, null, null, null)*/);
        
        List<String> whereClauses = new ArrayList<>();
        // Check state map for last max values
        if (stateMap != null && !stateMap.isEmpty() && maxValColumnNames != null) {
            IntStream.range(0, maxValColumnNames.size()).forEach((index) -> {
                String colName = maxValColumnNames.get(index);
                String maxValueKey = getStateKey(tableName, colName);
                String maxValue = stateMap.get(maxValueKey);
                if (!StringUtils.isEmpty(maxValue)) {
                    // Add a condition for the WHERE clause
                    whereClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, dbAdapter.getName()));
                }
            });
        }

        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(StringUtils.join(whereClauses, " AND "));
        }

        return query.toString();
    }

    public static class MaxValueResultSetRowCollector {
        final Map<String, String> newColMap;
        final Map<String, String> originalState;
        String tableName;

        public MaxValueResultSetRowCollector(String tableName, Map<String, String> stateMap) {
            this.originalState = stateMap;

            this.newColMap = new HashMap<>();
            this.newColMap.putAll(stateMap);

            this.tableName = tableName;
        }

        public void processRow(ResultSet resultSet) throws IOException {
            if (resultSet == null) {
                return;
            }
            try {
                // Iterate over the row, check-and-set max values
                final ResultSetMetaData meta = resultSet.getMetaData();
                final int nrOfColumns = meta.getColumnCount();
                if (nrOfColumns > 0) {
                    for (int i = 1; i <= nrOfColumns; i++) {
                        String colName = meta.getColumnName(i).toLowerCase();
                        String fullyQualifiedMaxValueKey = getStateKey(tableName, colName);
                        // Skip any columns we're not keeping track of or whose value is null
                        if (resultSet.getObject(i) == null) {
                            continue;
                        }
                        String maxValueString = newColMap.get(fullyQualifiedMaxValueKey);
                        // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                        // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                        // maximum value is observed, it will be stored under the fully-qualified key from then on.
                        if (StringUtils.isEmpty(maxValueString)) {
                            maxValueString = newColMap.get(colName);
                        }
                        String newMaxValueString = getMaxValueFromRow(resultSet, i, maxValueString);
                        if (newMaxValueString != null) {
                            newColMap.put(fullyQualifiedMaxValueKey, newMaxValueString);
                        }
                    }
                }
            } catch (ParseException | SQLException e) {
                throw new IOException(e);
            }
        }

        public void applyStateChanges() {
            this.originalState.putAll(this.newColMap);
        }
    }

    /**
     * Construct a key string for a corresponding state value.
     * @param prefix A prefix may contain database and table name, or just table name, this can be null
     * @param columnName A column name
     * @return a state key string
     */
    protected static String getStateKey(String prefix, String columnName) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix.toLowerCase());
            sb.append(NAMESPACE_DELIMITER);
        }
        if (columnName != null) {
            sb.append(columnName.toLowerCase());
        }
        return sb.toString();
    }

    protected Map<String, String> getDefaultMaxValueProperties(final ProcessContext context, final FlowFile flowFile) {
        final Map<String, String> defaultMaxValues = new HashMap<>();

        context.getProperties().forEach((k, v) -> {
            final String key = k.getName();

            if (key.startsWith(INITIAL_MAX_VALUE_PROP_START)) {
                defaultMaxValues.put(key.substring(INITIAL_MAX_VALUE_PROP_START.length()), context.getProperty(k).evaluateAttributeExpressions(flowFile).getValue());
            }
        });
        return defaultMaxValues;
    }
}

