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
package org.apache.nifi.processors.standard.util;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;

/**
 * JDBC / SQL common functions.
 */
public class JdbcCommon {

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream) throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, null, null);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName)
            throws SQLException, IOException {
        return convertToAvroStream(rs, outStream, recordName, null);
    }

    public static long convertToAvroStream(final ResultSet rs, final OutputStream outStream, String recordName, ResultSetRowCallback callback)
            throws SQLException, IOException {
        final Schema schema = createSchema(rs, recordName);
        final GenericRecord rec = new GenericData.Record(schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outStream);

            final ResultSetMetaData meta = rs.getMetaData();
            final int nrOfColumns = meta.getColumnCount();
            long nrOfRows = 0;
            while (rs.next()) {
                if (callback != null) {
                    callback.processRow(rs);
                }
                for (int i = 1; i <= nrOfColumns; i++) {
                    final int javaSqlType = meta.getColumnType(i);
                    final Object value = rs.getObject(i);

                    if (value == null) {
                        rec.put(i - 1, null);

                    } else if (javaSqlType == BINARY || javaSqlType == VARBINARY || javaSqlType == LONGVARBINARY || javaSqlType == ARRAY || javaSqlType == BLOB || javaSqlType == CLOB) {
                        // bytes requires little bit different handling
                        byte[] bytes = rs.getBytes(i);
                        ByteBuffer bb = ByteBuffer.wrap(bytes);
                        rec.put(i - 1, bb);

                    } else if (value instanceof Byte) {
                        // tinyint(1) type is returned by JDBC driver as java.sql.Types.TINYINT
                        // But value is returned by JDBC as java.lang.Byte
                        // (at least H2 JDBC works this way)
                        // direct put to avro record results:
                        // org.apache.avro.AvroRuntimeException: Unknown datum type java.lang.Byte
                        rec.put(i - 1, ((Byte) value).intValue());

                    } else if (value instanceof BigDecimal || value instanceof BigInteger) {
                        // Avro can't handle BigDecimal and BigInteger as numbers - it will throw an AvroRuntimeException such as: "Unknown datum type: java.math.BigDecimal: 38"
                        rec.put(i - 1, value.toString());

                    } else if (value instanceof Number || value instanceof Boolean) {
                        rec.put(i - 1, value);

                    } else {
                        // The different types that we support are numbers (int, long, double, float),
                        // as well as boolean values and Strings. Since Avro doesn't provide
                        // timestamp types, we want to convert those to Strings. So we will cast anything other
                        // than numbers or booleans to strings by using the toString() method.
                        rec.put(i - 1, value.toString());
                    }
                }
                dataFileWriter.append(rec);
                nrOfRows += 1;
            }

            return nrOfRows;
        }
    }

    public static Schema createSchema(final ResultSet rs) throws SQLException {
        return createSchema(rs, null);
    }

    /**
     * Creates an Avro schema from a result set. If the table/record name is known a priori and provided, use that as a
     * fallback for the record name if it cannot be retrieved from the result set, and finally fall back to a default value.
     *
     * @param rs         The result set to convert to Avro
     * @param recordName The a priori record name to use if it cannot be determined from the result set.
     * @return A Schema object representing the result set converted to an Avro record
     * @throws SQLException if any error occurs during conversion
     */
    public static Schema createSchema(final ResultSet rs, String recordName) throws SQLException {
        final ResultSetMetaData meta = rs.getMetaData();
        final int nrOfColumns = meta.getColumnCount();
        String tableName = StringUtils.isEmpty(recordName) ? "NiFi_ExecuteSQL_Record" : recordName;
        if (nrOfColumns > 0) {
            String tableNameFromMeta = meta.getTableName(1);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();

        /**
         * Some missing Avro types - Decimal, Date types. May need some additional work.
         */
        for (int i = 1; i <= nrOfColumns; i++) {
            switch (meta.getColumnType(i)) {
                case CHAR:
                case LONGNVARCHAR:
                case LONGVARCHAR:
                case NCHAR:
                case NVARCHAR:
                case VARCHAR:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BIT:
                case BOOLEAN:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                    break;

                case INTEGER:
                    if (meta.isSigned(i)) {
                        builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    } else {
                        builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    }
                    break;

                case SMALLINT:
                case TINYINT:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                    break;

                case BIGINT:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                    break;

                // java.sql.RowId is interface, is seems to be database
                // implementation specific, let's convert to String
                case ROWID:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case FLOAT:
                case REAL:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
                    break;

                case DOUBLE:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                    break;

                // Did not find direct suitable type, need to be clarified!!!!
                case DECIMAL:
                case NUMERIC:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                // Did not find direct suitable type, need to be clarified!!!!
                case DATE:
                case TIME:
                case TIMESTAMP:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case ARRAY:
                case BLOB:
                case CLOB:
                    builder.name(meta.getColumnName(i)).type().unionOf().nullBuilder().endNull().and().bytesType().endUnion().noDefault();
                    break;


                default:
                    throw new IllegalArgumentException("createSchema: Unknown SQL type " + meta.getColumnType(i) + " cannot be converted to Avro type");
            }
        }

        return builder.endRecord();
    }

    public static String getMaxValueFromRow(ResultSet resultSet,
                                            int columnIndex,
                                            Integer type,
                                            String maxValueString,
                                            String preProcessStrategy)
            throws ParseException, IOException, SQLException {

        // Skip any columns we're not keeping track of or whose value is null
        if (type == null || resultSet.getObject(columnIndex) == null) {
            return null;
        }

        switch (type) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                String colStringValue = resultSet.getString(columnIndex);
                if (maxValueString == null || colStringValue.compareTo(maxValueString) > 0) {
                    return colStringValue;
                }
                break;

            case INTEGER:
            case SMALLINT:
            case TINYINT:
                Integer colIntValue = resultSet.getInt(columnIndex);
                Integer maxIntValue = null;
                if (maxValueString != null) {
                    maxIntValue = Integer.valueOf(maxValueString);
                }
                if (maxIntValue == null || colIntValue > maxIntValue) {
                    return colIntValue.toString();
                }
                break;

            case BIGINT:
                Long colLongValue = resultSet.getLong(columnIndex);
                Long maxLongValue = null;
                if (maxValueString != null) {
                    maxLongValue = Long.valueOf(maxValueString);
                }
                if (maxLongValue == null || colLongValue > maxLongValue) {
                    return colLongValue.toString();
                }
                break;

            case FLOAT:
            case REAL:
            case DOUBLE:
                Double colDoubleValue = resultSet.getDouble(columnIndex);
                Double maxDoubleValue = null;
                if (maxValueString != null) {
                    maxDoubleValue = Double.valueOf(maxValueString);
                }
                if (maxDoubleValue == null || colDoubleValue > maxDoubleValue) {
                    return colDoubleValue.toString();
                }
                break;

            case DECIMAL:
            case NUMERIC:
                BigDecimal colBigDecimalValue = resultSet.getBigDecimal(columnIndex);
                BigDecimal maxBigDecimalValue = null;
                if (maxValueString != null) {
                    DecimalFormat df = new DecimalFormat();
                    df.setParseBigDecimal(true);
                    maxBigDecimalValue = (BigDecimal) df.parse(maxValueString);
                }
                if (maxBigDecimalValue == null || colBigDecimalValue.compareTo(maxBigDecimalValue) > 0) {
                    return colBigDecimalValue.toString();
                }
                break;

            case DATE:
                Date rawColDateValue = resultSet.getDate(columnIndex);
                java.sql.Date colDateValue = new java.sql.Date(rawColDateValue.getTime());
                java.sql.Date maxDateValue = null;
                if (maxValueString != null) {
                    maxDateValue = java.sql.Date.valueOf(maxValueString);
                }
                if (maxDateValue == null || colDateValue.after(maxDateValue)) {
                    return colDateValue.toString();
                }
                break;

            case TIME:
                Date rawColTimeValue = resultSet.getDate(columnIndex);
                java.sql.Time colTimeValue = new java.sql.Time(rawColTimeValue.getTime());
                java.sql.Time maxTimeValue = null;
                if (maxValueString != null) {
                    maxTimeValue = java.sql.Time.valueOf(maxValueString);
                }
                if (maxTimeValue == null || colTimeValue.after(maxTimeValue)) {
                    return colTimeValue.toString();
                }
                break;

            case TIMESTAMP:
                // Oracle timestamp queries must use literals in java.sql.Date format
                if ("Oracle".equals(preProcessStrategy)) {
                    Date rawColOracleTimestampValue = resultSet.getDate(columnIndex);
                    java.sql.Date oracleTimestampValue = new java.sql.Date(rawColOracleTimestampValue.getTime());
                    java.sql.Date maxOracleTimestampValue = null;
                    if (maxValueString != null) {
                        maxOracleTimestampValue = java.sql.Date.valueOf(maxValueString);
                    }
                    if (maxOracleTimestampValue == null || oracleTimestampValue.after(maxOracleTimestampValue)) {
                        return oracleTimestampValue.toString();
                    }
                } else {
                    Timestamp rawColTimestampValue = resultSet.getTimestamp(columnIndex);
                    java.sql.Timestamp colTimestampValue = new java.sql.Timestamp(rawColTimestampValue.getTime());
                    java.sql.Timestamp maxTimestampValue = null;
                    if (maxValueString != null) {
                        maxTimestampValue = java.sql.Timestamp.valueOf(maxValueString);
                    }
                    if (maxTimestampValue == null || colTimestampValue.after(maxTimestampValue)) {
                        return colTimestampValue.toString();
                    }
                }
                break;

            case BIT:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
            case CLOB:
            default:
                throw new IOException("Type for column " + columnIndex + " is not valid for maintaining maximum value");
        }
        return null;
    }

    /**
     * Returns a SQL literal for the given value based on its type. For example, values of character type need to be enclosed
     * in single quotes, whereas values of numeric type should not be.
     *
     * @param type  The JDBC type for the desired literal
     * @param value The value to be converted to a SQL literal
     * @return A String representing the given value as a literal of the given type
     */
    public static String getLiteralByType(int type, String value, String preProcessStrategy) {
        // Format value based on column type. For example, strings and timestamps need to be quoted
        switch (type) {
            // For string-represented values, put in single quotes
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
            case DATE:
            case TIME:
                return "'" + value + "'";
            case TIMESTAMP:
                // Timestamp literals in Oracle need to be cast with TO_DATE
                if ("Oracle".equals(preProcessStrategy)) {
                    return "to_date('" + value + "', 'yyyy-mm-dd HH24:MI:SS')";
                } else {
                    return "'" + value + "'";
                }
                // Else leave as is (numeric types, e.g.)
            default:
                return value;
        }
    }

    /**
     * An interface for callback methods which allows processing of a row during the convertToAvroStream() processing.
     * <b>IMPORTANT:</b> This method should only work on the row pointed at by the current ResultSet reference.
     * Advancing the cursor (e.g.) can cause rows to be skipped during Avro transformation.
     */
    public interface ResultSetRowCallback {
        void processRow(ResultSet resultSet) throws IOException;
    }
}
