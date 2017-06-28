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
class KotlinProcessor : Processor {

    val REL_TEST = new (Relationship::Builder)()
            .name("test")
            .description("A test relationship")
            .build();

    val descriptor = new (PropertyDescriptor.Builder()
            .name("test-attribute").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build()

    override fun initialize(context : ProcessorInitializationContext) {
    }

    override fun getRelationships() {
        return [REL_TEST] as Set
    }

    override fun onTrigger(context : ProcessContext, sessionFactory : ProcessSessionFactory) throws ProcessException {
        val session = sessionFactory.createSession()
        var flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        flowFile = session.putAttribute(flowFile, "from-content", "test content")
        // transfer
        session.transfer(flowFile, REL_TEST)
        session.commit()
    }

    override fun validate(context : ValidationContext) {
        return null
    }

    override fun getPropertyDescriptor(name : String) {
        return (name?.equals("test-attribute") ? descriptor : null)
    }

    override fun onPropertyModified(descriptor : PropertyDescriptor, oldValue : String, newValue : String) {

    }

    override fun getPropertyDescriptors() {
        return [descriptor] as List
    }

    override fun getIdentifier() {
        return null
    }
}

bindings["processor"] = new KotlinProcessor()