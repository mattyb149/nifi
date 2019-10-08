package org.apache.nifi.rules.engine;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.rules.Action;

import java.util.List;
import java.util.Map;

public class MockRulesEngineService extends AbstractConfigurableComponent implements RulesEngineService{

    private List<Action> actions;

    public MockRulesEngineService(List<Action> actions) {
        this.actions = actions;
    }

    @Override
    public List<Action> fireRules(Map<String, Object> facts) {
        return actions;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {
    }

    @Override
    public String getIdentifier() {
        return "MockRulesEngineService";
    }
}
