package org.apache.nifi.rules;

import java.util.Map;

public class Action {
    private String type;
    private Map<String,String> attributes;

    public Action() {
    }

    public Action(String type, Map<String, String> attributes) {
        this.type = type;
        this.attributes = attributes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}
