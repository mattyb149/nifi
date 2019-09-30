package org.apache.nifi.reporting.diagnostics;

import java.util.List;

public class Diagnostic {

    private String name;
    private List<Rule> rules;
    private Metrics metrics;

    public Diagnostic() {
    }

    public Diagnostic(String name, List<Rule> rules, Metrics metrics) {
        this.name = name;
        this.rules = rules;
        this.metrics = metrics;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public void setRules(List<Rule> rules) {
        this.rules = rules;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }
}
