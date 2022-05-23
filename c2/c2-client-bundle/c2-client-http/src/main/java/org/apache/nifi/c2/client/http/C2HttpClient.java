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
package org.apache.nifi.c2.client.http;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.api.C2Serializer;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2HttpClient implements C2Client {

    private static final Logger logger = LoggerFactory.getLogger(C2HttpClient.class);
    private static final MediaType MEDIA_TYPE_APPLICATION_JSON = MediaType.parse("application/json");

    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();
    private final C2ClientConfig clientConfig;
    private final C2Serializer serializer;

    public C2HttpClient(C2ClientConfig clientConfig, C2Serializer serializer) {
        super();
        this.clientConfig = clientConfig;
        this.serializer = serializer;
        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        // Set whether to follow redirects
        okHttpClientBuilder.followRedirects(true);

        // check if the ssl path is set and add the factory if so
        if (StringUtils.isNotBlank(clientConfig.getKeystoreFilename())) {
            try {
                setSslSocketFactory(okHttpClientBuilder);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        httpClientReference.set(okHttpClientBuilder.build());
    }

    @Override
    public Optional<C2HeartbeatResponse> publishHeartbeat(C2Heartbeat heartbeat) {
        return serializer.serialize(heartbeat).flatMap(this::sendHeartbeat);
    }

    private Optional<C2HeartbeatResponse> sendHeartbeat(String heartbeat) {
        logger.debug("Sending heartbeat to {}", clientConfig.getC2Url());

        Optional<C2HeartbeatResponse> c2HeartbeatResponse = Optional.empty();
        Request request = new Request.Builder()
            .post(RequestBody.create(heartbeat, MEDIA_TYPE_APPLICATION_JSON))
            .url(clientConfig.getC2Url())
            .build();

        try {
            Response heartbeatResponse = httpClientReference.get().newCall(request).execute();
            c2HeartbeatResponse = getResponseBody(heartbeatResponse).flatMap(response -> serializer.deserialize(response, C2HeartbeatResponse.class));
        } catch (IOException ce) {
            logger.error("Error while sending heartbeat", ce);
        }

        return c2HeartbeatResponse;
    }

    private Optional<String> getResponseBody(Response response) {
        String responseBody = null;

        try {
            responseBody = response.body().string();
            logger.debug("Received response (Status={}) {}", response.code(), responseBody);
        } catch (IOException e) {
            logger.error("Could not get response body: ", e);
        }

        return Optional.ofNullable(responseBody);
    }

    private void setSslSocketFactory(OkHttpClient.Builder okHttpClientBuilder) throws Exception {
        final String keystoreLocation = clientConfig.getKeystoreFilename();
        final String keystoreType = clientConfig.getKeystoreType().getType();
        final String keystorePass = clientConfig.getKeystorePass();

        assertKeystorePropertiesSet(keystoreLocation, keystorePass, keystoreType);

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore.load(keyStoreStream, keystorePass.toCharArray());
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass.toCharArray());

        // load truststore
        final String truststoreLocation = clientConfig.getTruststoreFilename();
        final String truststorePass = clientConfig.getTruststorePass();
        final String truststoreType = clientConfig.getTruststoreType().getType();
        assertTruststorePropertiesSet(truststoreLocation, truststorePass, truststoreType);

        KeyStore truststore = KeyStore.getInstance(truststoreType);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        truststore.load(new FileInputStream(truststoreLocation), truststorePass.toCharArray());
        trustManagerFactory.init(truststore);

        final X509TrustManager x509TrustManager;
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers[0] != null) {
            x509TrustManager = (X509TrustManager) trustManagers[0];
        } else {
            throw new IllegalStateException("List of trust managers is null");
        }

        SSLContext tempSslContext;
        try {
            tempSslContext = SSLContext.getInstance("TLS");
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Unable to use 'TLS' for the PullHttpChangeIngestor due to NoSuchAlgorithmException. Will attempt to use the default algorithm.", e);
            tempSslContext = SSLContext.getDefault();
        }

        final SSLContext sslContext = tempSslContext;
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        okHttpClientBuilder.sslSocketFactory(sslSocketFactory, x509TrustManager);
    }

    private void assertKeystorePropertiesSet(String location, String password, String type) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException(clientConfig.getKeystoreFilename() + " is null or is empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("The client's keystore filename is set but its password is not (or is empty). If the location is set, the password must also be.");
        }

        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("The client's keystore filename is set but its type is not (or is empty). If the location is set, the type must also be.");
        }
    }

    private void assertTruststorePropertiesSet(String location, String password, String type) {
        if (location == null || location.isEmpty()) {
            throw new IllegalArgumentException("The client's truststore filename is not set or is empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("The client's truststore filename is set but its password is not (or is empty). If the location is set, the password must also be.");
        }

        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("The client's truststore filename is set but its type is not (or is empty). If the location is set, the type must also be.");
        }
    }

    @Override
    public ByteBuffer retrieveUpdateContent(String flowUpdateUrl) {
        final Request.Builder requestBuilder = new Request.Builder()
                .get()
                .url(flowUpdateUrl);
        final Request request = requestBuilder.build();

        ResponseBody body;
        try (final Response response = httpClientReference.get().newCall(request).execute()) {
            logger.debug("Response received: {}", response);

            int code = response.code();
            if (code >= 400) {
                throw new IOException("Got response code " + code + " while trying to pull configuration: " + response.body().string());
            }

            body = response.body();

            if (body == null) {
                logger.warn("No body returned when pulling a new configuration");
                return null;
            }

            final ByteBuffer bodyByteBuffer = ByteBuffer.wrap(body.bytes());
            return bodyByteBuffer;

        } catch (Exception e) {
            logger.warn("Hit an exception while trying to pull", e);
            return null;
        }
    }

    @Override
    public void acknowledgeOperation(C2OperationAck operationAck) {
        logger.info("Performing acknowledgement request to {} for operation {}", clientConfig.getC2AckUrl(), operationAck.getOperationId());
        final ObjectMapper jacksonObjectMapper = new ObjectMapper();
        jacksonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        try {
            final String operationAckBody = jacksonObjectMapper.writeValueAsString(operationAck);

            final RequestBody requestBody = RequestBody.create(operationAckBody, MediaType.parse("application/json"));
            final Request.Builder requestBuilder = new Request.Builder()
                    .post(requestBody)
                    .url(clientConfig.getC2AckUrl());
            final Response heartbeatResponse = httpClientReference.get().newCall(requestBuilder.build()).execute();
            if (!heartbeatResponse.isSuccessful()) {
                logger.warn("Acknowledgement was not successful.");
            }
            logger.trace("Status on acknowledgement was {}", heartbeatResponse.code());
        } catch (Exception e) {
            logger.error("Could not transmit ack to c2 server", e);
        }
    }
}
