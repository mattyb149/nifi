package org.apache.nifi.reporting.diagnostics;

import java.util.List;

public class Metrics {

    private List<String> values;
    private String query;

    public Metrics() {
    }

    public Metrics(List<String> values, String query) {
        this.values = values;
        this.query = query;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

}
