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
package org.apache.nifi.c2.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class C2ClientBase implements C2Client {

    private static final Logger logger = LoggerFactory.getLogger(C2ClientBase.class);

    private final ObjectMapper objectMapper;

    public C2ClientBase() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    protected Optional<String> serialiseHeartbeat(C2Heartbeat heartbeat) {
        if (heartbeat == null) {
            logger.trace("Heartbeat was null. Returning empty");
            return Optional.empty();
        }

        String heartbeatString = null;
        try {
            heartbeatString = objectMapper.writeValueAsString(heartbeat);
            logger.trace("Serialized C2Heartbeat: {}", heartbeatString);
        } catch (JsonProcessingException e) {
            logger.error("Can't serialise C2Heartbeat: ", e);
        }

        return Optional.ofNullable(heartbeatString);
    }

    protected <T> Optional<T> deserialize(String content, Class<T> valueType) {
        if (content == null) {
            logger.trace("Content for deserialization was null. Returning empty.");
            return Optional.empty();
        }

        T responseObject = null;
        try {
            responseObject = objectMapper.readValue(content, valueType);
        } catch (JsonProcessingException e) {
            logger.error("Can't deserialize response object: ", e);
        }

        return Optional.ofNullable(responseObject);
    }
}
