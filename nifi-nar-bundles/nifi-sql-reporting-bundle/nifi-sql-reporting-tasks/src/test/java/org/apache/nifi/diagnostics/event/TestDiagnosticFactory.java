package org.apache.nifi.diagnostics.event;

import org.apache.nifi.reporting.diagnostics.Diagnostic;
import org.apache.nifi.reporting.diagnostics.DiagnosticFactory;
import org.apache.nifi.reporting.diagnostics.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestDiagnosticFactory {

    @Test
    public void testCreateDiagnosticsFromYaml(){
        try {
            String testYamlFile = "src/test/resources/test_diagnostics.yml";
            List<Diagnostic> diagnostics = DiagnosticFactory.createDiagnostics(testYamlFile, "YAML");
            assertEquals(2, diagnostics.size());
            assert confirmDiagnosticEntries(diagnostics);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testCreateDiagnosticsFromJson(){
        try {
            String testJsonFile = "src/test/resources/test_diagnostics.json";
            List<Diagnostic> diagnostics = DiagnosticFactory.createDiagnostics(testJsonFile, "JSON");
            assertEquals(2, diagnostics.size());
            assert confirmDiagnosticEntries(diagnostics);
        }catch (Exception ex){
            fail("Unexpected exception occurred: "+ex.getMessage());
        }
    }

    @Test
    public void testFakeTypeNotSupport(){
        try {
            List<Diagnostic> diagnostics = DiagnosticFactory.createDiagnostics("FAKEFILE", "FAKE");
        }catch (Exception ex){
            return;
        }
        fail("Exception shoudl have been thrown for unexpected type");
    }

    private boolean confirmDiagnosticEntries(List<Diagnostic> diagnostics){
        Diagnostic diagnostic1 = diagnostics.get(0);
        Diagnostic diagnostic2 = diagnostics.get(1);

        boolean checkDiagnostic = diagnostic1.getName().equals("Connection Assessment") && diagnostic2.getName().equals("Connection Assessment of Concern");

        List<Rule> rules1 = diagnostic1.getRules();
        List<Rule> rules2 = diagnostic2.getRules();

        checkDiagnostic = rules1.get(0).getName().equals("Queue Size") && rules1.get(0).getDescription().equals("Queue size check greater than 50")
                          && rules1.get(0).getPriority() == 1 && rules1.get(0).getCondition().equals("predictedQueuedCount > 50") && checkDiagnostic;

        checkDiagnostic = rules1.get(1).getName().equals("Time To Back Pressure") && rules1.get(1).getDescription().equals("Back pressure time less than 5 minutes")
                && rules1.get(1).getPriority() == 2 && rules1.get(1).getCondition().equals("predictedTimeToBytesBackpressureMillis < 300000") && checkDiagnostic;

        checkDiagnostic = rules2.get(0).getName().equals("Queue Size") && rules2.get(0).getDescription().equals("Queue size check greater than 1000")
                && rules2.get(0).getPriority() == 1 && rules2.get(0).getCondition().equals("predictedQueuedCount == 1000") && checkDiagnostic;

        checkDiagnostic = rules2.get(1).getName().equals("Time To Back Pressure") && rules2.get(1).getDescription().equals("Back pressure time less than 5 minutes")
                && rules2.get(1).getPriority() == 2 && rules2.get(1).getCondition().equals("predictedTimeToBytesBackpressureMillis > 100000000") && checkDiagnostic;

        return checkDiagnostic;

    }

}
