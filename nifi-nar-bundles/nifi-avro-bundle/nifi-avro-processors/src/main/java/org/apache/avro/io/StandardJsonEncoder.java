/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

/**
 * An {@link Encoder} for Standard JSON data encoding, which mimics the output of Avro's GenericData.toString() method
 */
public class StandardJsonEncoder extends JsonEncoder {

    private static MinimalPrettyPrinter JSON_PRINTER = new MinimalPrettyPrinter() {
        @Override
        public void writeObjectFieldValueSeparator(JsonGenerator jg) throws IOException {
            jg.writeRaw(": ");
        }

        @Override
        public void writeObjectEntrySeparator(JsonGenerator jg) throws IOException {
            jg.writeRaw(", ");
        }
    };

    public StandardJsonEncoder(Schema sc, OutputStream out) throws IOException {
        super(sc, new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8).setPrettyPrinter(JSON_PRINTER));
    }

    @Override
    public void writeIndex(int unionIndex) throws IOException {
        parser.advance(Symbol.UNION);
        Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
        Symbol symbol = top.getSymbol(unionIndex);
        parser.pushSymbol(symbol);
    }
}