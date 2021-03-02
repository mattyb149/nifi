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
package org.apache.nifi.script.impl;


import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.concurrent.atomic.AtomicReference;

public class KotlinScriptEngineConfigurator extends AbstractModuleClassloaderConfigurator {

    private final AtomicReference<CompiledScript> compiledScriptRef = new AtomicReference<>();

    private static final String PRELOADS =
            "import org.apache.nifi.components.*\n"
                    + "import org.apache.nifi.flowfile.*\n"
                    + "import org.apache.nifi.processor.*\n"
                    + "import org.apache.nifi.processor.exception.*\n"
                    + "import org.apache.nifi.processor.io.*\n"
                    + "import org.apache.nifi.processor.util.*\n"
                    + "import org.apache.nifi.processors.script.*\n"
                    + "import org.apache.nifi.logging.ComponentLog\n"
                    + "import org.apache.nifi.script.*\n"
                    + "import org.apache.nifi.lookup.*\n"
                    + "import javax.script.Bindings\n";

    @Override
    public String getScriptEngineName() {
        return "kotlin";
    }

    @Override
    public Object init(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        // Always compile when first run
        if (engine != null && compiledScriptRef.get() == null) {
            // Add prefix for common imports and bindings
            final CompiledScript compiled = ((Compilable) engine).compile(PRELOADS + scriptBody);
            compiledScriptRef.set(compiled);
        }
        return compiledScriptRef.get();
    }

    @Override
    public Object eval(ScriptEngine engine, String scriptBody, String[] modulePaths) throws ScriptException {
        Object returnValue = null;
        if (engine != null) {
            final CompiledScript existing = compiledScriptRef.get();
            if (existing == null) {
                throw new ScriptException("Kotlin script has not been compiled, the processor must be restarted.");
            }
            returnValue = compiledScriptRef.get().eval();
        }
        return returnValue;
    }
}
