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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_BLOCK_PADDING;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_BLOCK_SIZE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_WRITE_FORMAT;

/**
 * Utility methods for ORC support (conversion from Avro, conversion to Hive types, e.g.
 */
public class NiFiOrcUtils {

    public static Object convertToORCObject(TypeInfo typeInfo, Object o) {
        return convertToORCObject(typeInfo, o, false);
    }

    public static Object convertToORCObject(TypeInfo typeInfo, Object o, final boolean hiveFieldNames) {
        if (o != null) {
            if (typeInfo instanceof UnionTypeInfo) {
                OrcUnion union = new OrcUnion();
                // Avro uses Utf8 and GenericData.EnumSymbol objects instead of Strings. This is handled in other places in the method, but here
                // we need to determine the union types from the objects, so choose String.class if the object is one of those Avro classes
                Class clazzToCompareTo = o.getClass();
                if (o instanceof org.apache.avro.util.Utf8 || o instanceof GenericData.EnumSymbol) {
                    clazzToCompareTo = String.class;
                }
                // Need to find which of the union types correspond to the primitive object
                TypeInfo objectTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(
                        ObjectInspectorFactory.getReflectionObjectInspector(clazzToCompareTo, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
                List<TypeInfo> unionTypeInfos = ((UnionTypeInfo) typeInfo).getAllUnionObjectTypeInfos();

                int index = 0;
                while (index < unionTypeInfos.size() && !unionTypeInfos.get(index).equals(objectTypeInfo)) {
                    index++;
                }
                if (index < unionTypeInfos.size()) {
                    union.set((byte) index, convertToORCObject(objectTypeInfo, o, hiveFieldNames));
                } else {
                    throw new IllegalArgumentException("Object Type for class " + o.getClass().getName() + " not in Union declaration");
                }
                return union;
            }
            if (o instanceof Integer) {
                return new IntWritable((int) o);
            }
            if (o instanceof Boolean) {
                return new BooleanWritable((boolean) o);
            }
            if (o instanceof Long) {
                return new LongWritable((long) o);
            }
            if (o instanceof Float) {
                return new FloatWritable((float) o);
            }
            if (o instanceof Double) {
                return new DoubleWritable((double) o);
            }
            if (o instanceof String || o instanceof Utf8 || o instanceof GenericData.EnumSymbol) {
                return new Text(o.toString());
            }
            if (o instanceof ByteBuffer) {
                return new BytesWritable(((ByteBuffer) o).array());
            }
            if (o instanceof Timestamp) {
                return new TimestampWritable((Timestamp) o);
            }
            if (o instanceof Date) {
                return new DateWritable((Date) o);
            }
            if (o instanceof Object[]) {
                Object[] objArray = (Object[]) o;
                if(TypeInfoFactory.binaryTypeInfo.equals(typeInfo)) {
                    byte[] dest = new byte[objArray.length];
                    for(int i=0;i<objArray.length;i++) {
                        dest[i] = (byte) objArray[i];
                    }
                    return new BytesWritable(dest);
                } else {
                    // If not binary, assume a list of objects
                    TypeInfo listTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
                    return Arrays.stream(objArray)
                            .map(o1 -> convertToORCObject(listTypeInfo, o1, hiveFieldNames))
                            .collect(Collectors.toList());
                }
            }
            if (o instanceof int[]) {
                int[] intArray = (int[]) o;
                return Arrays.stream(intArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("int"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof long[]) {
                long[] longArray = (long[]) o;
                return Arrays.stream(longArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("bigint"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof float[]) {
                float[] floatArray = (float[]) o;
                return IntStream.range(0, floatArray.length)
                        .mapToDouble(i -> floatArray[i])
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("float"), (float) element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof double[]) {
                double[] doubleArray = (double[]) o;
                return Arrays.stream(doubleArray)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("double"), element, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof boolean[]) {
                boolean[] booleanArray = (boolean[]) o;
                return IntStream.range(0, booleanArray.length)
                        .map(i -> booleanArray[i] ? 1 : 0)
                        .mapToObj((element) -> convertToORCObject(TypeInfoFactory.getPrimitiveTypeInfo("boolean"), element == 1, hiveFieldNames))
                        .collect(Collectors.toList());
            }
            if (o instanceof GenericData.Array) {
                GenericData.Array array = ((GenericData.Array) o);
                // The type information in this case is interpreted as a List
                TypeInfo listTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
                return array.stream().map((element) -> convertToORCObject(listTypeInfo, element, hiveFieldNames)).collect(Collectors.toList());
            }
            if (o instanceof List) {
                return o;
            }
            if (o instanceof Map) {
                Map map = new HashMap();
                MapTypeInfo mapTypeInfo = ((MapTypeInfo) typeInfo);
                TypeInfo keyInfo = mapTypeInfo.getMapKeyTypeInfo();
                TypeInfo valueInfo = mapTypeInfo.getMapValueTypeInfo();
                // Unions are not allowed as key/value types, so if we convert the key and value objects,
                // they should return Writable objects
                ((Map) o).forEach((key, value) -> {
                    Object keyObject = convertToORCObject(keyInfo, key, hiveFieldNames);
                    Object valueObject = convertToORCObject(valueInfo, value, hiveFieldNames);
                    if (keyObject == null) {
                        throw new IllegalArgumentException("Maps' key cannot be null");
                    }
                    map.put(keyObject, valueObject);
                });
                return map;
            }
            if (o instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) o;
                TypeInfo recordSchema = NiFiOrcUtils.getOrcField(record.getSchema(), hiveFieldNames);
                List<Schema.Field> recordFields = record.getSchema().getFields();
                if (recordFields != null) {
                    Object[] fieldObjects = new Object[recordFields.size()];
                    for (int i = 0; i < recordFields.size(); i++) {
                        Schema.Field field = recordFields.get(i);
                        Schema fieldSchema = field.schema();
                        Object fieldObject = record.get(field.name());
                        fieldObjects[i] = NiFiOrcUtils.convertToORCObject(NiFiOrcUtils.getOrcField(fieldSchema, hiveFieldNames), fieldObject, hiveFieldNames);
                    }
                    return NiFiOrcUtils.createOrcStruct(recordSchema, fieldObjects);
                }
            }
            if (o instanceof Record) {
                Record record = (Record) o;
                TypeInfo recordSchema = NiFiOrcUtils.getOrcSchema(record.getSchema(), hiveFieldNames);
                List<RecordField> recordFields = record.getSchema().getFields();
                if (recordFields != null) {
                    Object[] fieldObjects = new Object[recordFields.size()];
                    for (int i = 0; i < recordFields.size(); i++) {
                        RecordField field = recordFields.get(i);
                        DataType dataType = field.getDataType();
                        Object fieldObject = record.getValue(field);
                        fieldObjects[i] = convertToORCObject(NiFiOrcUtils.getOrcField(dataType, hiveFieldNames), fieldObject, hiveFieldNames);
                    }
                    return NiFiOrcUtils.createOrcStruct(recordSchema, fieldObjects);
                }
                return null;
            }
            throw new IllegalArgumentException("Error converting object of type " + o.getClass().getName() + " to ORC type " + typeInfo.getTypeName());
        } else {
            return null;
        }
    }


    /**
     * Create an object of OrcStruct given a TypeInfo and a list of objects
     *
     * @param typeInfo The TypeInfo object representing the ORC record schema
     * @param objs     ORC objects/Writables
     * @return an OrcStruct containing the specified objects for the specified schema
     */
    public static OrcStruct createOrcStruct(TypeInfo typeInfo, Object... objs) {
        SettableStructObjectInspector oi = (SettableStructObjectInspector) OrcStruct
                .createObjectInspector(typeInfo);
        List<StructField> fields = (List<StructField>) oi.getAllStructFieldRefs();
        OrcStruct result = (OrcStruct) oi.create();
        result.setNumFields(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            oi.setStructFieldData(result, fields.get(i), objs[i]);
        }
        return result;
    }

    public static String normalizeHiveTableName(String name) {
        return name.replaceAll("[\\. ]", "_");
    }

    public static String generateHiveDDL(RecordSchema recordSchema, String tableName, boolean hiveFieldNames) {
        StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE IF NOT EXISTS ");
        sb.append(tableName);
        sb.append(" (");
        List<String> hiveColumns = new ArrayList<>();
        List<RecordField> fields = recordSchema.getFields();
        if (fields != null) {
            hiveColumns.addAll(
                    fields.stream().map(field -> (hiveFieldNames ? field.getFieldName().toLowerCase() : field.getFieldName()) + " "
                            + getHiveTypeFromRecordFieldType(field.getDataType(), hiveFieldNames)).collect(Collectors.toList()));
        }
        sb.append(StringUtils.join(hiveColumns, ", "));
        sb.append(") STORED AS ORC");
        return sb.toString();
    }

    public static String generateHiveDDL(Schema avroSchema, String tableName) {
        return generateHiveDDL(avroSchema, tableName, false);
    }

    public static String generateHiveDDL(Schema avroSchema, String tableName, boolean hiveFieldNames) {
        Schema.Type schemaType = avroSchema.getType();
        StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE IF NOT EXISTS ");
        sb.append(tableName);
        sb.append(" (");
        if (Schema.Type.RECORD.equals(schemaType)) {
            List<String> hiveColumns = new ArrayList<>();
            List<Schema.Field> fields = avroSchema.getFields();
            if (fields != null) {
                hiveColumns.addAll(
                        fields.stream().map(field -> (hiveFieldNames ? field.name().toLowerCase() : field.name()) + " "
                                + getHiveTypeFromAvroType(field.schema(), hiveFieldNames)).collect(Collectors.toList()));
            }
            sb.append(StringUtils.join(hiveColumns, ", "));
            sb.append(") STORED AS ORC");
            return sb.toString();
        } else {
            throw new IllegalArgumentException("Avro schema is of type " + schemaType.getName() + ", not RECORD");
        }
    }

    public static TypeInfo getOrcField(Schema fieldSchema) throws IllegalArgumentException {
        return getOrcField(fieldSchema, false);
    }

    public static TypeInfo getOrcSchema(final RecordSchema recordSchema, final boolean hiveFieldNames) throws IllegalArgumentException {
        List<RecordField> recordFields = recordSchema.getFields();
        if (recordFields != null) {
            List<String> orcFieldNames = new ArrayList<>(recordFields.size());
            List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
            recordFields.forEach(recordField -> {
                String fieldName = recordField.getFieldName();
                orcFieldNames.add(fieldName);
                orcFields.add(getOrcTypeFromRecordFieldType(recordField.getDataType(), hiveFieldNames));
            });
            return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
        }
        return null;
    }

    public static TypeInfo getOrcField(DataType dataType, boolean hiveFieldNames) throws IllegalArgumentException {
        if (dataType == null) {
            return null;
        }

        RecordFieldType fieldType = dataType.getFieldType();
        if (RecordFieldType.INT.equals(fieldType)
                || RecordFieldType.LONG.equals(fieldType)
                || RecordFieldType.BOOLEAN.equals(fieldType)
                || RecordFieldType.DOUBLE.equals(fieldType)
                || RecordFieldType.FLOAT.equals(fieldType)
                || RecordFieldType.STRING.equals(fieldType)) {
            return getPrimitiveOrcTypeFromPrimitiveFieldType(dataType);
        }
        if (RecordFieldType.DATE.equals(fieldType)) {
            return TypeInfoFactory.dateTypeInfo;
        }
        if (RecordFieldType.TIME.equals(fieldType)) {
            return TypeInfoFactory.intTypeInfo;
        }
        if (RecordFieldType.TIMESTAMP.equals(fieldType)) {
            return TypeInfoFactory.timestampTypeInfo;
        }
        if (RecordFieldType.ARRAY.equals(fieldType)) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return TypeInfoFactory.getPrimitiveTypeInfo("binary");
            }
            return TypeInfoFactory.getListTypeInfo(getOrcField(arrayDataType.getElementType(), hiveFieldNames));
        }
        if (RecordFieldType.CHOICE.equals(fieldType)) {
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> unionFieldSchemas = choiceDataType.getPossibleSubTypes();

            if (unionFieldSchemas != null) {
                // Ignore null types in union
                List<TypeInfo> orcFields = unionFieldSchemas.stream()
                        .map((it) -> NiFiOrcUtils.getOrcField(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // Flatten the field if the union only has one non-null element
                if (orcFields.size() == 1) {
                    return orcFields.get(0);
                } else {
                    return TypeInfoFactory.getUnionTypeInfo(orcFields);
                }
            }
            return null;
        }
        if (RecordFieldType.MAP.equals(fieldType)) {
            MapDataType mapDataType = (MapDataType) dataType;
            return TypeInfoFactory.getMapTypeInfo(
                    getPrimitiveOrcTypeFromPrimitiveFieldType(RecordFieldType.STRING.getDataType()),
                    getOrcField(mapDataType.getValueType(), hiveFieldNames));
        }
        if (RecordFieldType.RECORD.equals(fieldType)) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> orcFieldNames = new ArrayList<>(recordFields.size());
                List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
                recordFields.forEach(recordField -> {
                    String fieldName = hiveFieldNames ? recordField.getFieldName().toLowerCase() : recordField.getFieldName();
                    orcFieldNames.add(fieldName);
                    orcFields.add(getOrcField(recordField.getDataType(), hiveFieldNames));
                });
                return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
            }
            return null;
        }

        throw new IllegalArgumentException("Did not recognize field type " + fieldType.name());
    }

    public static TypeInfo getPrimitiveOrcTypeFromPrimitiveFieldType(DataType rawDataType) throws IllegalArgumentException {
        if (rawDataType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        RecordFieldType fieldType = rawDataType.getFieldType();
        if (RecordFieldType.INT.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("int");
        }
        if (RecordFieldType.LONG.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
        }
        if (RecordFieldType.BOOLEAN.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
        }
        if (RecordFieldType.DOUBLE.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("double");
        }
        if (RecordFieldType.FLOAT.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("float");
        }
        if (RecordFieldType.STRING.equals(fieldType)) {
            return TypeInfoFactory.getPrimitiveTypeInfo("string");
        }

        throw new IllegalArgumentException("Field type " + fieldType.name() + " is not a primitive type");
    }

    public static TypeInfo getOrcTypeFromRecordFieldType(DataType dataType, final boolean hiveFieldNames) {

        if (RecordFieldType.INT.getDataType().equals(dataType)) {
            return TypeInfoFactory.intTypeInfo;
        } else if (RecordFieldType.LONG.getDataType().equals(dataType)) {
            return TypeInfoFactory.longTypeInfo;
        } else if (RecordFieldType.of("bigint").getDataType().equals(dataType)) {
            return TypeInfoFactory.stringTypeInfo;
        } else if (RecordFieldType.BOOLEAN.getDataType().equals(dataType)) {
            return TypeInfoFactory.booleanTypeInfo;
        } else if (RecordFieldType.DOUBLE.getDataType().equals(dataType)) {
            return TypeInfoFactory.doubleTypeInfo;
        } else if (RecordFieldType.FLOAT.getDataType().equals(dataType)) {
            return TypeInfoFactory.floatTypeInfo;
        } else if (RecordFieldType.STRING.getDataType().equals(dataType)) {
            return TypeInfoFactory.stringTypeInfo;
        } else if (RecordFieldType.DATE.getDataType().equals(dataType)) {
            return TypeInfoFactory.dateTypeInfo;
        } else if (RecordFieldType.TIME.getDataType().equals(dataType)) {
            return TypeInfoFactory.intTypeInfo;
        } else if (RecordFieldType.TIMESTAMP.getDataType().equals(dataType)) {
            return TypeInfoFactory.timestampTypeInfo;
        } else if (RecordFieldType.CHOICE.getDataType().equals(dataType.getFieldType().getDataType())) {
            // ^^^ Need to go back to the field type to get the "base" data type for CHOICE, as CHOICE types are only equal if their subtypes are
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> unionFieldTypes = choiceDataType.getPossibleSubTypes();
            if (unionFieldTypes != null) {
                // Ignore null types in union
                List<TypeInfo> orcFields = unionFieldTypes.stream()
                        .map((it) -> NiFiOrcUtils.getOrcTypeFromRecordFieldType(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // Flatten the field if the union only has one non-null element
                if (orcFields.size() == 1) {
                    return orcFields.get(0);
                } else {
                    return TypeInfoFactory.getUnionTypeInfo(orcFields);
                }
            }
            return null;
        } else if (RecordFieldType.MAP.getDataType().equals(dataType)) {
            MapDataType mapDataType = (MapDataType) dataType;
            return TypeInfoFactory.getMapTypeInfo(
                    TypeInfoFactory.getPrimitiveTypeInfo("string"),
                    getOrcTypeFromRecordFieldType(mapDataType.getValueType(), hiveFieldNames));
        } else if (RecordFieldType.ARRAY.getDataType().equals(dataType)) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return TypeInfoFactory.getPrimitiveTypeInfo("binary");
            } else {
                return TypeInfoFactory.getListTypeInfo(getOrcTypeFromRecordFieldType(arrayDataType.getElementType(), hiveFieldNames));
            }
        } else if (RecordFieldType.RECORD.getDataType().equals(dataType)) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> orcFieldNames = new ArrayList<>(recordFields.size());
                List<TypeInfo> orcFields = new ArrayList<>(recordFields.size());
                recordFields.forEach(recordField -> {
                    String fieldName = recordField.getFieldName();
                    orcFieldNames.add(fieldName);
                    orcFields.add(getOrcTypeFromRecordFieldType(recordField.getDataType(), hiveFieldNames));
                });
                return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
            }
            return null;
        } else {
            throw new IllegalArgumentException("Record type " + dataType.toString() + " is not a primitive type");
        }
    }

    public static String getHiveTypeFromRecordFieldType(final DataType dataType, final boolean hiveFieldNames) {
        if (dataType == null) {
            throw new IllegalArgumentException("Data type is null");
        }

        if (RecordFieldType.INT.getDataType().equals(dataType)) {
            return "INT";
        } else if (RecordFieldType.LONG.getDataType().equals(dataType)) {
            return "BIGINT";
        } else if (RecordFieldType.BOOLEAN.getDataType().equals(dataType)) {
            return "BOOLEAN";
        } else if (RecordFieldType.DOUBLE.getDataType().equals(dataType)) {
            return "DOUBLE";
        } else if (RecordFieldType.FLOAT.getDataType().equals(dataType)) {
            return "FLOAT";
        } else if (RecordFieldType.STRING.getDataType().equals(dataType)) {
            return "STRING";
        } else if (RecordFieldType.DATE.getDataType().equals(dataType)) {
            return "DATE";
        } else if (RecordFieldType.TIME.getDataType().equals(dataType)) {
            return "INT";
        } else if (RecordFieldType.TIMESTAMP.getDataType().equals(dataType)) {
            return "TIMESTAMP";
        } else if (RecordFieldType.CHOICE.getDataType().equals(dataType.getFieldType().getDataType())) {
            // ^^^ Need to go back to the field type to get the "base" data type for CHOICE, as CHOICE types are only equal if their subtypes are
            ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            List<DataType> unionFieldTypes = choiceDataType.getPossibleSubTypes();
            if (unionFieldTypes != null) {
                // Ignore null types in union
                List<String> hiveFields = unionFieldTypes.stream()
                        .map((it) -> NiFiOrcUtils.getHiveTypeFromRecordFieldType(it, hiveFieldNames))
                        .collect(Collectors.toList());

                // Flatten the field if the union only has one non-null element
                if (hiveFields.size() == 1) {
                    return hiveFields.get(0);
                } else {
                    return "UNIONTYPE<" + StringUtils.join(hiveFields, ", ") + ">";
                }
            }
            return null;
        } else if (RecordFieldType.MAP.getDataType().equals(dataType)) {
            MapDataType mapDataType = (MapDataType) dataType;
            return "MAP<STRING, " + getHiveTypeFromRecordFieldType(mapDataType.getValueType(), hiveFieldNames) + ">";
        } else if (RecordFieldType.ARRAY.getDataType().equals(dataType)) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            if (RecordFieldType.BYTE.getDataType().equals(arrayDataType.getElementType())) {
                return "BINARY";
            } else {
                return "ARRAY<" + getHiveTypeFromRecordFieldType(arrayDataType.getElementType(), hiveFieldNames) + ">";
            }
        } else if (RecordFieldType.RECORD.getDataType().equals(dataType)) {
            RecordDataType recordDataType = (RecordDataType) dataType;
            List<RecordField> recordFields = recordDataType.getChildSchema().getFields();
            if (recordFields != null) {
                List<String> hiveFields = recordFields.stream().map(
                        recordField -> recordField.getFieldName() + ":" + getHiveTypeFromRecordFieldType(recordField.getDataType(), hiveFieldNames)).collect(Collectors.toList());
                return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
            }
        }

        throw new IllegalArgumentException("Error converting RecordField type " + dataType.toString() + " to Hive type");
    }

    public static TypeInfo getOrcField(Schema fieldSchema, boolean hiveFieldNames) throws IllegalArgumentException {
        Schema.Type fieldType = fieldSchema.getType();

        switch (fieldType) {
            case INT:
            case LONG:
            case BOOLEAN:
            case BYTES:
            case DOUBLE:
            case FLOAT:
            case STRING:
            case NULL:
                return getPrimitiveOrcTypeFromPrimitiveAvroType(fieldType);

            case UNION:
                List<Schema> unionFieldSchemas = fieldSchema.getTypes();

                if (unionFieldSchemas != null) {
                    // Ignore null types in union
                    List<TypeInfo> orcFields = unionFieldSchemas.stream().filter(
                            unionFieldSchema -> !Schema.Type.NULL.equals(unionFieldSchema.getType()))
                            .map((it) -> NiFiOrcUtils.getOrcField(it, hiveFieldNames))
                            .collect(Collectors.toList());

                    // Flatten the field if the union only has one non-null element
                    if (orcFields.size() == 1) {
                        return orcFields.get(0);
                    } else {
                        return TypeInfoFactory.getUnionTypeInfo(orcFields);
                    }
                }
                return null;

            case ARRAY:
                return TypeInfoFactory.getListTypeInfo(getOrcField(fieldSchema.getElementType(), hiveFieldNames));

            case MAP:
                return TypeInfoFactory.getMapTypeInfo(
                        getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING),
                        getOrcField(fieldSchema.getValueType(), hiveFieldNames));

            case RECORD:
                List<Schema.Field> avroFields = fieldSchema.getFields();
                if (avroFields != null) {
                    List<String> orcFieldNames = new ArrayList<>(avroFields.size());
                    List<TypeInfo> orcFields = new ArrayList<>(avroFields.size());
                    avroFields.forEach(avroField -> {
                        String fieldName = avroField.name();
                        orcFieldNames.add(fieldName);
                        orcFields.add(getOrcField(avroField.schema(), hiveFieldNames));
                    });
                    return TypeInfoFactory.getStructTypeInfo(orcFieldNames, orcFields);
                }
                return null;

            case ENUM:
                // An enum value is just a String for ORC/Hive
                return getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING);

            default:
                throw new IllegalArgumentException("Did not recognize Avro type " + fieldType.getName());
        }

    }

    public static TypeInfo getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type avroType) throws IllegalArgumentException {
        if (avroType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        switch (avroType) {
            case INT:
                return TypeInfoFactory.getPrimitiveTypeInfo("int");
            case LONG:
                return TypeInfoFactory.getPrimitiveTypeInfo("bigint");
            case BOOLEAN:
            case NULL: // ORC has no null type, so just pick the smallest. All values are necessarily null.
                return TypeInfoFactory.getPrimitiveTypeInfo("boolean");
            case BYTES:
                return TypeInfoFactory.getPrimitiveTypeInfo("binary");
            case DOUBLE:
                return TypeInfoFactory.getPrimitiveTypeInfo("double");
            case FLOAT:
                return TypeInfoFactory.getPrimitiveTypeInfo("float");
            case STRING:
                return TypeInfoFactory.getPrimitiveTypeInfo("string");
            default:
                throw new IllegalArgumentException("Avro type " + avroType.getName() + " is not a primitive type");
        }
    }

    public static String getHiveTypeFromAvroType(Schema avroSchema) {
        return getHiveTypeFromAvroType(avroSchema, false);
    }

    public static String getHiveTypeFromAvroType(Schema avroSchema, boolean hiveFieldNames) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro schema is null");
        }

        Schema.Type avroType = avroSchema.getType();

        switch (avroType) {
            case INT:
                return "INT";
            case LONG:
                return "BIGINT";
            case BOOLEAN:
            case NULL: // Hive has no null type, we picked boolean as the ORC type so use it for Hive DDL too. All values are necessarily null.
                return "BOOLEAN";
            case BYTES:
                return "BINARY";
            case DOUBLE:
                return "DOUBLE";
            case FLOAT:
                return "FLOAT";
            case STRING:
            case ENUM:
                return "STRING";
            case UNION:
                List<Schema> unionFieldSchemas = avroSchema.getTypes();
                if (unionFieldSchemas != null) {
                    List<String> hiveFields = new ArrayList<>();
                    for (Schema unionFieldSchema : unionFieldSchemas) {
                        Schema.Type unionFieldSchemaType = unionFieldSchema.getType();
                        // Ignore null types in union
                        if (!Schema.Type.NULL.equals(unionFieldSchemaType)) {
                            hiveFields.add(getHiveTypeFromAvroType(unionFieldSchema, hiveFieldNames));
                        }
                    }
                    // Flatten the field if the union only has one non-null element
                    return (hiveFields.size() == 1)
                            ? hiveFields.get(0)
                            : "UNIONTYPE<" + StringUtils.join(hiveFields, ", ") + ">";

                }
                break;
            case MAP:
                return "MAP<STRING, " + getHiveTypeFromAvroType(avroSchema.getValueType(), hiveFieldNames) + ">";
            case ARRAY:
                return "ARRAY<" + getHiveTypeFromAvroType(avroSchema.getElementType(), hiveFieldNames) + ">";
            case RECORD:
                List<Schema.Field> recordFields = avroSchema.getFields();
                if (recordFields != null) {
                    List<String> hiveFields = recordFields.stream().map(
                            recordField -> recordField.name() + ":" + getHiveTypeFromAvroType(recordField.schema(), hiveFieldNames)).collect(Collectors.toList());
                    return "STRUCT<" + StringUtils.join(hiveFields, ", ") + ">";
                }
                break;
            default:
                break;
        }

        throw new IllegalArgumentException("Error converting Avro type " + avroType.getName() + " to Hive type");
    }


    public static OrcFlowFileWriter createWriter(OutputStream flowFileOutputStream,
                                                 Path path,
                                                 Configuration conf,
                                                 TypeInfo orcSchema,
                                                 long stripeSize,
                                                 CompressionKind compress,
                                                 int bufferSize) throws IOException {

        int rowIndexStride = HiveConf.getIntVar(conf, HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);

        boolean addBlockPadding = HiveConf.getBoolVar(conf, HIVE_ORC_DEFAULT_BLOCK_PADDING);

        String versionName = HiveConf.getVar(conf, HIVE_ORC_WRITE_FORMAT);
        OrcFile.Version versionValue = (versionName == null)
                ? OrcFile.Version.CURRENT
                : OrcFile.Version.byName(versionName);

        OrcFile.EncodingStrategy encodingStrategy;
        String enString = conf.get(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname);
        if (enString == null) {
            encodingStrategy = OrcFile.EncodingStrategy.SPEED;
        } else {
            encodingStrategy = OrcFile.EncodingStrategy.valueOf(enString);
        }

        OrcFile.CompressionStrategy compressionStrategy;
        String compString = conf.get(HiveConf.ConfVars.HIVE_ORC_COMPRESSION_STRATEGY.varname);
        if (compString == null) {
            compressionStrategy = OrcFile.CompressionStrategy.SPEED;
        } else {
            compressionStrategy = OrcFile.CompressionStrategy.valueOf(compString);
        }

        float paddingTolerance;
        paddingTolerance = conf.getFloat(HiveConf.ConfVars.HIVE_ORC_BLOCK_PADDING_TOLERANCE.varname,
                HiveConf.ConfVars.HIVE_ORC_BLOCK_PADDING_TOLERANCE.defaultFloatVal);

        long blockSizeValue = HiveConf.getLongVar(conf, HIVE_ORC_DEFAULT_BLOCK_SIZE);

        double bloomFilterFpp = BloomFilterIO.DEFAULT_FPP;

        ObjectInspector inspector = OrcStruct.createObjectInspector(orcSchema);

        return new OrcFlowFileWriter(flowFileOutputStream,
                path,
                conf,
                inspector,
                stripeSize,
                compress,
                bufferSize,
                rowIndexStride,
                getMemoryManager(conf),
                addBlockPadding,
                versionValue,
                null, // no callback
                encodingStrategy,
                compressionStrategy,
                paddingTolerance,
                blockSizeValue,
                null, // no Bloom Filter column names
                bloomFilterFpp);
    }

    // Creates a writer directly to a FileSystem Path
    public static Writer createWriter(
            Path path,
            Configuration conf,
            TypeInfo orcSchema,
            long stripeSize,
            CompressionKind compress,
            int bufferSize) throws IOException {

        int rowIndexStride = HiveConf.getIntVar(conf, HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);

        boolean addBlockPadding = HiveConf.getBoolVar(conf, HIVE_ORC_DEFAULT_BLOCK_PADDING);

        String versionName = HiveConf.getVar(conf, HIVE_ORC_WRITE_FORMAT);
        OrcFile.Version versionValue = (versionName == null)
                ? OrcFile.Version.CURRENT
                : OrcFile.Version.byName(versionName);

        OrcFile.EncodingStrategy encodingStrategy;
        String enString = conf.get(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname);
        if (enString == null) {
            encodingStrategy = OrcFile.EncodingStrategy.SPEED;
        } else {
            encodingStrategy = OrcFile.EncodingStrategy.valueOf(enString);
        }

        float paddingTolerance;
        paddingTolerance = conf.getFloat(HiveConf.ConfVars.HIVE_ORC_BLOCK_PADDING_TOLERANCE.varname,
                HiveConf.ConfVars.HIVE_ORC_BLOCK_PADDING_TOLERANCE.defaultFloatVal);

        long blockSizeValue = HiveConf.getLongVar(conf, HIVE_ORC_DEFAULT_BLOCK_SIZE);

        double bloomFilterFpp = BloomFilterIO.DEFAULT_FPP;

        ObjectInspector inspector = OrcStruct.createObjectInspector(orcSchema);

        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
                .rowIndexStride(rowIndexStride)
                .blockPadding(addBlockPadding)
                .version(versionValue)
                .encodingStrategy(encodingStrategy)
                .paddingTolerance(paddingTolerance)
                .blockSize(blockSizeValue)
                .bloomFilterFpp(bloomFilterFpp)
                .memory(getMemoryManager(conf))
                .inspector(inspector)
                .stripeSize(stripeSize)
                .bufferSize(bufferSize)
                .compress(compress);

        return OrcFile.createWriter(path, writerOptions);
    }

    private static MemoryManager memoryManager = null;

    private static synchronized MemoryManager getMemoryManager(Configuration conf) {
        if (memoryManager == null) {
            memoryManager = new MemoryManager(conf);
        }
        return memoryManager;
    }
}