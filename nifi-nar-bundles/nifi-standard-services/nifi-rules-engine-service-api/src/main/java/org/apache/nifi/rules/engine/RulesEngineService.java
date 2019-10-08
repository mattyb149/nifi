package org.apache.nifi.rules.engine;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.rules.Action;

import java.util.List;
import java.util.Map;

public interface RulesEngineService extends ControllerService {

    List<Action> fireRules(Map<String, Object> facts);

}
