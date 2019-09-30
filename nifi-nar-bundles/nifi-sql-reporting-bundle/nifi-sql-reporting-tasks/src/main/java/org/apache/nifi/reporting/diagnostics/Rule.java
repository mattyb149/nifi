package org.apache.nifi.reporting.diagnostics;

import java.util.List;

public class Rule {

    private String name;
    private String description;
    private Integer priority;
    private String condition;
    private List<Action> actions;

    public Rule() {
    }

    public Rule(String name, String description, Integer priority, String condition, List<Action> actions) {
        this.name = name;
        this.description = description;
        this.priority = priority;
        this.condition = condition;
        this.actions = actions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }
}
