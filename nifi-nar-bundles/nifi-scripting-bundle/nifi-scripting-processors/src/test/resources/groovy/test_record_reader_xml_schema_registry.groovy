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

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.MalformedRecordException
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordSchema
import org.apache.nifi.schemaregistry.services.SchemaRegistry
import org.apache.nifi.serialization.record.SchemaIdentifier

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME


class GroovyXmlSchemaRecordReader implements RecordReader {

    def recordIterator
    RecordSchema recordSchema

    GroovyXmlSchemaRecordReader(final String recordTag, final RecordSchema schema, final InputStream inputStream) {
        recordSchema = schema
        def xml = new XmlSlurper().parse(inputStream)
        // Change the XML fields to a MapRecord for each incoming record
        recordIterator = xml[recordTag].collect { r ->
            // Create a map of field names to values, using the field names from the schema as keys into the XML object
            def fields = recordSchema.fieldNames.inject([:]) { result, fieldName ->
                result[fieldName] = r[fieldName].toString()
                result
            }
            new MapRecord(recordSchema, fields)
        }.iterator()
    }

    Record nextRecord(boolean coerceTypes, boolean dropUnknown) throws IOException, MalformedRecordException {
        return recordIterator?.hasNext() ? recordIterator.next() : null
    }

    RecordSchema getSchema() throws MalformedRecordException {
        return recordSchema
    }

    void close() throws IOException {
    }
}

class GroovyXmlSchemaRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    // Will be set by the ScriptedRecordReaderFactory
    ConfigurationContext configurationContext

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        super.supportedPropertyDescriptors + SCHEMA_REGISTRY + SCHEMA_NAME
    }

    RecordReader createRecordReader(Map<String, String> variables, InputStream inputStream, ComponentLog logger)
            throws MalformedRecordException, IOException, SchemaNotFoundException {

        def schemaRegistry = configurationContext.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry)
        def schemaName = configurationContext.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(variables).value
        def recordSchema = schemaRegistry.retrieveSchema(SchemaIdentifier.builder().name(schemaName).build())
        if (!recordSchema) return null
        return new GroovyXmlSchemaRecordReader(variables.get('record.tag'), recordSchema, inputStream)
    }
}

// Create an instance of RecordReaderFactory called "reader", this is the entry point for ScriptedReader
reader = new GroovyXmlSchemaRecordReaderFactory()
