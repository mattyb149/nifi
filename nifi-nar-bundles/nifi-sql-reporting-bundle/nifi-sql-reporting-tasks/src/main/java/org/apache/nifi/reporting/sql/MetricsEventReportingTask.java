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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.reporting.metrics.MetricsEventHandlerService;
import org.apache.nifi.reporting.metrics.event.handlers.EventHandler;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Tags({"reporting", "rules", "status", "connection", "processor", "jvm", "metrics", "history", "bulletin", "sql"})
@CapabilityDescription("Triggers rules-driven events based on metrics values ")
public class MetricsEventReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor RULES_ENGINE = new PropertyDescriptor.Builder()
            .name("rules-engine-service")
            .displayName("Rules Engine Service")
            .description("Specifies the Controller Service to use for applying rules to metrics.")
            .identifiesControllerService(RulesEngineService.class)
            .required(true)
            .build();

    static final PropertyDescriptor METRICS_EVENT_HANDLER = new PropertyDescriptor.Builder()
            .name("metrics-event-handler")
            .displayName("Metrics Event Handler")
            .description("Specifies the Controller Service to use for handling eventsgenerated from metrics.")
            .identifiesControllerService(MetricsEventHandlerService.class)
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

    private List<PropertyDescriptor> properties;
    private MetricsQueryService metricsQueryService;
    private volatile MetricsEventHandlerService metricsEventHandlerService;
    private volatile RulesEngineService rulesEngineService;

    @Override
    protected void init(final ReportingInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(METRICS_EVENT_HANDLER);
        properties.add(RULES_ENGINE);
        properties.add(QUERY);
        this.properties = Collections.unmodifiableList(properties);
        metricsQueryService = new MetricsSqlQueryService(getLogger());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        try {
            metricsEventHandlerService = context.getProperty(METRICS_EVENT_HANDLER).asControllerService(MetricsEventHandlerService.class);
            rulesEngineService = context.getProperty(RULES_ENGINE).asControllerService(RulesEngineService.class);
            final String query = context.getProperty(QUERY).evaluateAttributeExpressions().getValue();
            fireRules(context, rulesEngineService, query);
        } catch (Exception e) {
            getLogger().error("Error opening loading rules: {}", new Object[]{e.getMessage()}, e);
        }
    }

    private void fireRules(ReportingContext context, RulesEngineService engine, String query) throws Exception {
        QueryResult queryResult = metricsQueryService.query(context, query);
        getLogger().debug("Executing query: {}", new Object[]{ query });
        ResultSetRecordSet recordSet = metricsQueryService.getResultSetRecordSet(queryResult);
        Record record;
        try {
            while ((record = recordSet.next()) != null) {
                final Map<String, Object> facts = new HashMap<>();
                for (String fieldName : record.getRawFieldNames()) {
                    facts.put(fieldName, record.getValue(fieldName));
                }
                List<Action> actions = engine.fireRules(facts);
                if(actions == null ||  actions.isEmpty()){
                    getLogger().debug("No actions required for provided facts.");
                } else {
                    metricsEventHandlerService.process(facts, actions, createCustomAlertHandler(context));
                }
            }
        } finally {
            metricsQueryService.closeQuietly(recordSet);
        }
    }

    private Map<String, EventHandler> createCustomAlertHandler(final ReportingContext context) {
        Map<String, EventHandler> handlerMap = new HashMap<>();
        EventHandler alertHandler = (metrics, attributes) -> {
            if (context.getBulletinRepository() != null) {
                final String category = attributes.getOrDefault("category", "Metric Event");
                final String message = attributes.getOrDefault("message", "Metric Event Alert");
                final String level = attributes.getOrDefault("severity", attributes.getOrDefault("logLevel", "info"));
                Severity severity;
                try {
                    severity = Severity.valueOf(level.toUpperCase());
                } catch (IllegalArgumentException iae) {
                    severity = Severity.INFO;
                }
                BulletinRepository bulletinRepository = context.getBulletinRepository();
                bulletinRepository.addBulletin(context.createBulletin(category, severity, message));
            }
        };
        handlerMap.put("ALERT", alertHandler);
        return handlerMap;
    }

}
