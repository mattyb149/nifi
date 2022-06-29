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
package org.apache.nifi.controllerservices.airtable;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

@Tags({"airtable", "client", "service"})
@CapabilityDescription("A controller service for accessing the Airtable API. This is used for purposes such as retrieving rows from a table.")
public class AirtableClientService implements AirtableService extends AbstractControllerService {

    private static String API_URL_BASE = "https://api.airtable.com/v0/";

    public static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("airtable-service-api-url")
            .displayName("Airtable API URL")
            .description("Airtable REST API URL for querying table data." +
                    " It only needs to be changed if Airtable changes its API URL.")
            .required(true)
            .defaultValue(API_URL_BASE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("airtable-service-ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections (i.e. the HTTPS REST API for Airtable)")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .addValidator(Validator.VALID)
            .build();

}
