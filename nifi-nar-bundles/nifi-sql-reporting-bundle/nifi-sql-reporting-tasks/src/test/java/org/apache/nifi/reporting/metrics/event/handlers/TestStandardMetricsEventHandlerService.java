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
package org.apache.nifi.reporting.metrics.event.handlers;


import com.google.common.collect.Lists;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.metrics.MetricsEventHandlerService;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;

public class TestStandardMetricsEventHandlerService {

    @Test
    public void testWarningLogged() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();

        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: This is a warning\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";


        attributes.put("logLevel","warn");
        attributes.put("message","This is a warning");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));

        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action));

        String logMessage = mockComponentLog.getWarnMessage();

        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testAlertWithBulletinLevel() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: This should be sent as an alert!\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        attributes.put("logLevel","FAKE");
        attributes.put("message","This should be sent as an alert!");

        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));
        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action));
        String logMessage = mockComponentLog.getWarnMessage();

        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testAlertNoBulletinLevel() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: This should be sent as an alert!\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        attributes.put("message","This should be sent as an alert!");

        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));
        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action));
        String logMessage = mockComponentLog.getWarnMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testAlertInvalidBulletinLevel() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: This should be sent as an alert!\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        attributes.put("message","This should be sent as an alert!");

        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));
        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action));
        String logMessage = mockComponentLog.getWarnMessage();

        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testHandler() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);
        EventHandler mockHandler = Mockito.mock(EventHandler.class);
        Map<String,EventHandler> mockHandlerMap = new HashMap<>();

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();

        attributes.put("message","This should be sent as an alert!");
        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));
        mockHandlerMap.put("ALERT", mockHandler);
        final Action action = new Action();
        action.setType("ALERT");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action),mockHandlerMap);
        Mockito.verify(mockHandler, Mockito.times(1)).execute(anyMap(), anyMap());
    }

    @Test
    public void testNoLogAttributesProvided() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: Event triggered log.\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));
        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action));
        String logMessage = mockComponentLog.getInfoMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testInvalidLogLevelProvided() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardMetricsEventHandlerService service = new MockStandardMetricsEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();

        attributes.put("logLevel","FAKE");

        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: Event triggered log.\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("standard-metric-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MetricsEventHandlerService eventHandlerService = (MetricsEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("standard-metric-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardMetricsEventHandlerService.class));
        final Action action = new Action();
        action.setType("LOG");
        action.setAttributes(attributes);
        eventHandlerService.process(metrics, Lists.newArrayList(action));
        String logMessage = mockComponentLog.getInfoMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    private class MockStandardMetricsEventHandlerService extends StandardMetricsEventHandlerService {

        private ComponentLog testLogger;

        public MockStandardMetricsEventHandlerService(ComponentLog testLogger) {
            super();
            this.testLogger = testLogger;
        }

        @Override
        protected ComponentLog getLogger() {
            return testLogger;
        }
    }


}
