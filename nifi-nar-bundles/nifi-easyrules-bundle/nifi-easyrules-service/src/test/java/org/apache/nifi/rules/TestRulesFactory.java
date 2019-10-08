package org.apache.nifi.rules;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestRulesFactory {
    @Test
    public void testCreateDiagnosticsFromYaml(){
        try {
            String testYamlFile = "src/test/resources/test_rules.yml";
            List<Rule> rules = RulesFactory.createRules(testYamlFile,"YAML");
            assertEquals(2, rules.size());
            assert confirmEntries(rules);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateDiagnosticsFromJson(){
        try {
            String testJsonFile = "src/test/resources/test_rules.json";
            List<Rule> rules = RulesFactory.createRules(testJsonFile,"JSON");
            assertEquals(2, rules.size());
            assert confirmEntries(rules);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testFakeTypeNotSupport(){
        try {
            RulesFactory.createRules("FAKEFILE", "FAKE");
        }catch (Exception ex){
            return;
        }
        fail("Exception shoudl have been thrown for unexpected type");
    }

    private boolean confirmEntries(List<Rule> rules){
        Rule rule1= rules.get(0);
        Rule rule2 = rules.get(1);

        boolean checkDiagnostic = rule1.getName().equals("Queue Size") && rule1.getDescription().equals("Queue size check greater than 50")
                && rule1.getPriority() == 1 && rule1.getCondition().equals("predictedQueuedCount > 50");

        checkDiagnostic = rule2.getName().equals("Time To Back Pressure") && rule2.getDescription().equals("Back pressure time less than 5 minutes")
                && rule2.getPriority() == 2 && rule2.getCondition().equals("predictedTimeToBytesBackpressureMillis >= 300000") && checkDiagnostic;

        return checkDiagnostic;

    }
}
