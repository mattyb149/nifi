package org.apache.nifi.diagnostics.event.handlers;


import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.diagnostics.DiagnosticEventHandlerService;
import org.apache.nifi.reporting.diagnostics.event.handlers.StandardDiagnosticEventHandlerService;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class TestStandardDiagnosticEventHandlerService {

    @Test
    public void testWarningLogged() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardDiagnosticEventHandlerService service = new MockStandardDiagnosticEventHandlerService(mockComponentLog);

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

        runner.addControllerService("default-diagnostic-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final DiagnosticEventHandlerService eventHandlerService = (DiagnosticEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("default-diagnostic-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardDiagnosticEventHandlerService.class));
        eventHandlerService.sendData(metrics, DiagnosticEventHandlerService.EventAction.LOG,attributes);
        String logMessage = mockComponentLog.getWarnMessage();

        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testNoLogAttributesProvided() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardDiagnosticEventHandlerService service = new MockStandardDiagnosticEventHandlerService(mockComponentLog);

        final Map<String,String> attributes = new HashMap<>();
        final Map<String,Object> metrics = new HashMap<>();
        final String expectedMessage = "--------------------------------------------------\n" +
                "Event Message: Event triggered log.\n" +
                "Event Metrics:\n" +
                "Field: cpu, Value: 90\n" +
                "Field: jvmHeap, Value: 1000000";

        metrics.put("jvmHeap","1000000");
        metrics.put("cpu","90");

        runner.addControllerService("default-diagnostic-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final DiagnosticEventHandlerService eventHandlerService = (DiagnosticEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("default-diagnostic-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardDiagnosticEventHandlerService.class));
        eventHandlerService.sendData(metrics, DiagnosticEventHandlerService.EventAction.LOG,attributes);
        String logMessage = mockComponentLog.getInfoMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    @Test
    public void testInvalidLogLevelProvided() throws InitializationException, IOException{

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final MockComponentLog mockComponentLog = new MockComponentLog();
        final StandardDiagnosticEventHandlerService service = new MockStandardDiagnosticEventHandlerService(mockComponentLog);

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

        runner.addControllerService("default-diagnostic-event-handler-service",service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final DiagnosticEventHandlerService eventHandlerService = (DiagnosticEventHandlerService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("default-diagnostic-event-handler-service");

        assertThat(eventHandlerService, instanceOf(StandardDiagnosticEventHandlerService.class));
        eventHandlerService.sendData(metrics, DiagnosticEventHandlerService.EventAction.LOG,attributes);
        String logMessage = mockComponentLog.getInfoMessage();
        assertTrue(StringUtils.isNotEmpty(logMessage));
        assertEquals(expectedMessage,logMessage);

    }

    private class MockStandardDiagnosticEventHandlerService extends StandardDiagnosticEventHandlerService {

        private ComponentLog testLogger;

        public MockStandardDiagnosticEventHandlerService(ComponentLog testLogger) {
            super();
            this.testLogger = testLogger;
        }

        @Override
        protected ComponentLog getLogger() {
            return testLogger;
        }
    }


}
