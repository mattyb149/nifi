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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.reporting.diagnostics.Action;
import org.apache.nifi.reporting.diagnostics.Diagnostic;
import org.apache.nifi.reporting.diagnostics.DiagnosticEventHandlerService;
import org.apache.nifi.reporting.diagnostics.DiagnosticFactory;
import org.apache.nifi.reporting.diagnostics.Metrics;
import org.apache.nifi.reporting.diagnostics.Rule;
import org.apache.nifi.reporting.diagnostics.event.handlers.EventHandler;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.core.RuleBuilder;
import org.jeasy.rules.mvel.MVELCondition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Tags({"reporting", "rules", "status", "connection", "processor", "jvm", "metrics", "history", "bulletin", "sql", "diagnostic"})
@CapabilityDescription("Triggers rules-driven events based on metrics values ")
public class DiagnosticRulesReportingTask extends AbstractReportingTask {

    private static final AllowableValue YAML = new AllowableValue("YAML", "YAML", "YAML file configuration type.");
    private static final AllowableValue JSON = new AllowableValue("JSON", "JSON", "JSON file configuration type.");

    static final PropertyDescriptor RULES_FILE_PATH = new PropertyDescriptor.Builder()
            .name("rules-file-path")
            .displayName("Rules File Path")
            .description("Rules File Location")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor RULES_FILE_TYPE = new PropertyDescriptor.Builder()
            .name("rules-file-type")
            .displayName("Rules File Type")
            .description("File type for rules definition. Supported file types are YAML and JSON")
            .required(true)
            .allowableValues(YAML, JSON)
            .defaultValue(YAML.getValue())
            .build();

    static final PropertyDescriptor DIAGNOSTIC_EVENT_HANDLER = new PropertyDescriptor.Builder()
            .name("diagnostic-event-handler")
            .displayName("Diagnostic Event Handler")
            .description("Specifies the Controller Service to use for handling events from diagnostics.")
            .identifiesControllerService(DiagnosticEventHandlerService.class)
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;
    private MetricsQueryService metricsQueryService;
    private volatile DiagnosticEventHandlerService diagnosticEventHandlerService;

    @Override
    protected void init(final ReportingInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RULES_FILE_TYPE);
        properties.add(RULES_FILE_PATH);
        properties.add(DIAGNOSTIC_EVENT_HANDLER);
        this.properties = Collections.unmodifiableList(properties);
        metricsQueryService = new MetricsSqlQueryService(getLogger());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {

        diagnosticEventHandlerService = context.getProperty(DIAGNOSTIC_EVENT_HANDLER).asControllerService(DiagnosticEventHandlerService.class);
        final String rulesFile = context.getProperty(RULES_FILE_PATH).getValue();
        final String rulesFileType = context.getProperty(RULES_FILE_TYPE).getValue();

        try {

            List<Diagnostic> diagnostics = DiagnosticFactory.createDiagnostics(rulesFile, rulesFileType);

            if (diagnostics == null || diagnostics.isEmpty()) {
                getLogger().warn("No diagnostics available - confirm configuration file has content!");
            } else {

                final RulesEngine rulesEngine = new DefaultRulesEngine();

                diagnostics.forEach(diagnostic -> {

                    final Rules rules = getRules(diagnostic.getRules(), context);

                    try {
                        fireRules(context, rulesEngine, rules, diagnostic.getMetrics());
                    } catch (Exception e) {
                        getLogger().error("Error creating attempting to process metrics: ", new Object[]{e.getMessage()}, e);
                    }

                });
            }
        } catch (Exception e) {
            getLogger().error("Error opening loading rules", new Object[]{e.getMessage()}, e);
        }

    }

    private Rules getRules(List<Rule> diagnosticRules, ReportingContext context) {
        final Rules rules = new Rules();

        diagnosticRules.forEach(diagnosticRule -> {

            RuleBuilder ruleBuilder = new RuleBuilder();
            MVELCondition condition = new MVELCondition(diagnosticRule.getCondition());
            ruleBuilder.name(diagnosticRule.getName())
                    .description(diagnosticRule.getDescription())
                    .priority(diagnosticRule.getPriority())
                    .when(condition);

            for (Action action : diagnosticRule.getActions()) {
                ruleBuilder.then(facts -> diagnosticEventHandlerService.sendData(facts.asMap(),
                        DiagnosticEventHandlerService.EventAction.valueOf(action.getType()),
                        action.getAttributes(), createCustomAlertHandler(context)));
            }

            rules.register(ruleBuilder.build());
        });

        return rules;
    }

    private void fireRules(ReportingContext context, final RulesEngine engine, Rules rules, Metrics metrics) throws Exception {

        QueryResult queryResult = metricsQueryService.query(context, metrics.getQuery());
        getLogger().debug("Executing query: {}", new Object[]{metrics.getQuery()});
        ResultSetRecordSet recordSet = metricsQueryService.getResultSetRecordSet(queryResult);
        Record record;
        try {
            while ((record = recordSet.next()) != null) {
                final Facts facts = new Facts();
                for (String fieldName : metrics.getValues()) {
                    facts.put(fieldName, record.getValue(fieldName));
                }
                engine.fire(rules, facts);
            }
        } finally {
            metricsQueryService.closeQuietly(recordSet);
        }

    }

    private Map<DiagnosticEventHandlerService.EventAction, EventHandler> createCustomAlertHandler(final ReportingContext context) {
        Map<DiagnosticEventHandlerService.EventAction, EventHandler> handlerMap = new HashMap<>();
        EventHandler alertHandler = (metrics, attributes) -> {
            if (context.getBulletinRepository() != null) {
                final String category = attributes.getOrDefault("category", "Diagnostic Event");
                final String message = attributes.getOrDefault("message", "Diagnostic Event Alert");
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
        handlerMap.put(DiagnosticEventHandlerService.EventAction.ALERT, alertHandler);
        return handlerMap;
    }

}
