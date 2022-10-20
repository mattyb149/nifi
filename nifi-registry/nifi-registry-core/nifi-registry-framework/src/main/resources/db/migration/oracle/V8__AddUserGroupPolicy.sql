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

-- UserGroupProvider tables

CREATE TABLE UGP_USER (
    IDENTIFIER VARCHAR(50) PRIMARY KEY,
    IDENTITY VARCHAR(767) NOT NULL UNIQUE
);

CREATE TABLE UGP_GROUP (
    IDENTIFIER VARCHAR(50) PRIMARY KEY,
    IDENTITY VARCHAR(767) NOT NULL UNIQUE
);

-- There is no FK constraint from USER_IDENTIFIER to the UGP_USER table because users from multiple providers may be
-- put into a group here, so it may not always be a user from the UGP_USER table
CREATE TABLE UGP_USER_GROUP (
    USER_IDENTIFIER VARCHAR(50) NOT NULL,
    GROUP_IDENTIFIER VARCHAR(50) NOT NULL,
    CONSTRAINT PK__UGP_USER_GROUP PRIMARY KEY (USER_IDENTIFIER, GROUP_IDENTIFIER),
    FOREIGN KEY (GROUP_IDENTIFIER) REFERENCES UGP_GROUP(IDENTIFIER) ON DELETE CASCADE
);

-- AccessPolicyProvider tables

CREATE TABLE APP_POLICY (
    IDENTIFIER VARCHAR(50) PRIMARY KEY,
    "RESOURCE" VARCHAR(700) NOT NULL,
    ACTION VARCHAR(50) NOT NULL,
    CONSTRAINT UNIQUE__APP_POLICY_RESOURCE_ACTION UNIQUE ("RESOURCE", ACTION)
);

CREATE TABLE APP_POLICY_USER (
    POLICY_IDENTIFIER VARCHAR(50) NOT NULL,
    USER_IDENTIFIER VARCHAR(50) NOT NULL,
    CONSTRAINT PK__APP_POLICY_USER PRIMARY KEY (POLICY_IDENTIFIER, USER_IDENTIFIER),
    FOREIGN KEY (POLICY_IDENTIFIER) REFERENCES APP_POLICY(IDENTIFIER) ON DELETE CASCADE
);

CREATE TABLE APP_POLICY_GROUP (
    POLICY_IDENTIFIER VARCHAR(50) NOT NULL,
    GROUP_IDENTIFIER VARCHAR(50) NOT NULL,
    CONSTRAINT PK__APP_POLICY_GROUP PRIMARY KEY (POLICY_IDENTIFIER, GROUP_IDENTIFIER),
    FOREIGN KEY (POLICY_IDENTIFIER) REFERENCES APP_POLICY(IDENTIFIER) ON DELETE CASCADE
);