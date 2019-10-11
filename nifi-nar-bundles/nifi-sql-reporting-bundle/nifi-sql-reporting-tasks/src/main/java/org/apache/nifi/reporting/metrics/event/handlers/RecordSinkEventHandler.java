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
package org.apache.nifi.reporting.metrics.event.handlers;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordSinkEventHandler extends AbstractEventHandler {

    final private RecordSinkService recordSinkService;

    public RecordSinkEventHandler(ComponentLog logger, RecordSinkService recordSinkService) {
        super(logger);
        this.recordSinkService = recordSinkService;
    }

    @Override
    public void execute(Map<String, Object> metrics, Map<String, String> attributes) {

        boolean sendZeroResults = attributes.containsKey("sentZeroResults") && Boolean.parseBoolean(attributes.get("sendZeroResults"));
        final RecordSet recordSet = getRecordSet(metrics);

        try {
            WriteResult result = recordSinkService.sendData(recordSet, attributes, sendZeroResults);
            logger.debug("Records written to sink service: {}", new Object[]{result.getRecordCount()});

        }catch (Exception ex){
            logger.warn("Exception encountered when attempting to send metrics", ex);
        }


    }

    private RecordSet getRecordSet(Map<String, Object> metrics){
        List<RecordField> recordFields = metrics.keySet().stream().map( key ->
            new RecordField(key, RecordFieldType.STRING.getDataType())
        ).collect(Collectors.toList());
        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
        return new ListRecordSet(recordSchema, Arrays.asList( new MapRecord(recordSchema,metrics)));
    }

}
