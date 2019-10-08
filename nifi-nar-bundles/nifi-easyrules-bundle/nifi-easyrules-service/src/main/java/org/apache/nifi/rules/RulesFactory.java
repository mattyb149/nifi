/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.rules;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class RulesFactory {

    enum FileType {
        YAML, JSON;
    }

    public static List<Rule> createRules(String ruleFile, String ruleFileType) throws Exception{
        FileType type = FileType.valueOf(ruleFileType.toUpperCase());
        if (type.equals(FileType.YAML)) {
            return yamlToRules(ruleFile);
        } else if (type.equals(FileType.JSON)) {
            return jsonToRules(ruleFile);
        } else {
            return null;
        }
    }

    protected static List<Rule> yamlToRules(String rulesFile) throws FileNotFoundException {
        List<Rule> rules = new ArrayList<>();
        Yaml yaml = new Yaml(new Constructor(Rule.class));
        File yamlFile = new File(rulesFile);
        InputStream inputStream = new FileInputStream(yamlFile);
        for (Object object : yaml.loadAll(inputStream)) {
            if (object instanceof Rule) {
                rules.add((Rule) object);
            }
        }
        return rules;
    }

    protected static List<Rule> jsonToRules(String rulesFile) throws Exception {
        List<Rule> rules;
        Gson gson = new Gson();
        InputStreamReader isr = new InputStreamReader(new FileInputStream(rulesFile));
        Type rulesListType = new TypeToken<ArrayList<Rule>>() {}.getType();
        rules = gson.fromJson(isr, rulesListType);
        return rules;
    }
}
