package org.apache.nifi.rules.engine;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestEasyRulesEngineService {

    @Test
    public void testYamlNiFiRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_rules.yml");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "YAML");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",300000);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 1);
    }

    @Test
    public void testJsonNiFiRules() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final RulesEngineService service = new MockEasyRulesEngineService();
        runner.addControllerService("easy-rules-engine-service-test",service);
        runner.setProperty(service, EasyRulesEngineService.RULES_FILE_PATH, "src/test/resources/test_rules.json");
        runner.setProperty(service,EasyRulesEngineService.RULES_FILE_TYPE, "JSON");
        runner.enableControllerService(service);
        runner.assertValid(service);
        Map<String, Object> facts = new HashMap<>();
        facts.put("predictedQueuedCount",60);
        facts.put("predictedTimeToBytesBackpressureMillis",300000);
        List<Action> actions = service.fireRules(facts);
        assertNotNull(actions);
        assertEquals(actions.size(), 2);
    }

    private class MockEasyRulesEngineService extends EasyRulesEngineService {

    }

}
