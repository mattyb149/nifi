-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE BUCKET (
    ID VARCHAR(50) PRIMARY KEY,
    NAME VARCHAR(767) NOT NULL UNIQUE,
    "DESCRIPTION" VARCHAR(4000),
    CREATED TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE BUCKET_ITEM (
    ID VARCHAR(50) PRIMARY KEY,
    NAME VARCHAR(1000) NOT NULL,
    DESCRIPTION VARCHAR(4000),
    CREATED TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    MODIFIED TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    ITEM_TYPE VARCHAR(50) NOT NULL,
    BUCKET_ID VARCHAR(50) NOT NULL,
    FOREIGN KEY(BUCKET_ID) REFERENCES BUCKET(ID) ON DELETE CASCADE
);

CREATE TABLE FLOW (
    ID VARCHAR(50) PRIMARY KEY,
    FOREIGN KEY (ID) REFERENCES BUCKET_ITEM(ID) ON DELETE CASCADE
);

CREATE TABLE FLOW_SNAPSHOT (
    FLOW_ID VARCHAR(50) NOT NULL,
    VERSION INT NOT NULL,
    CREATED TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    CREATED_BY VARCHAR(1000) NOT NULL,
    "COMMENTS" VARCHAR(4000),
    CONSTRAINT PK__FLOW_SNAPSHOT_FLOW_ID_AND_VERSION PRIMARY KEY (FLOW_ID, VERSION),
    FOREIGN KEY (FLOW_ID) REFERENCES FLOW(ID) ON DELETE CASCADE
);

CREATE TABLE SIGNING_KEY (
    ID VARCHAR(50) PRIMARY KEY,
    TENANT_IDENTITY VARCHAR(767) NOT NULL UNIQUE,
    KEY_VALUE VARCHAR(50) NOT NULL
);