/*
 * Copyright 2023 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hive.bigquery.connector;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.input.udfs.*;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.protobuf.DescriptorProtos;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent class for the functionality that may differ based on the Hive version used. The
 * non-abstract methods provide default functionality. Children class are expected to implement the
 * abstract methods, as well as override some methods to customize behavior for the relevant Hive
 * versions if necessary.
 */
public abstract class HiveCompat {

  protected static String hiveUDFPackage = "org.apache.hadoop.hive.ql.udf.";
  protected static int hiveUDFPackageLength = hiveUDFPackage.length();

  protected final Cache<Object, Object> cache = CacheBuilder.newBuilder().build();

  private static final List<String> SIMPLE_TYPES =
      ImmutableList.of(
          "tinyint",
          "smallint",
          "int",
          "bigint",
          "float",
          "double",
          "char",
          "varchar",
          "string",
          "boolean");

  private static final Logger LOG = LoggerFactory.getLogger(HiveCompat.class);

  private static HiveCompat instance = null;

  public static HiveCompat getInstance() {
    if (instance == null) {
      ServiceLoader<HiveCompat> serviceLoader = ServiceLoader.load(HiveCompat.class);
      instance =
          Streams.stream(serviceLoader.iterator())
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              "Could not load instance of '%s', please check the META-INF/services directory in the connector's jar",
                              HiveCompat.class.getCanonicalName())));
    }
    return instance;
  }

  public abstract Object convertHiveTimeUnitToBq(
      ObjectInspector objectInspector, Object hiveValue, String writeMethod);

  public abstract Object convertTimeUnitFromArrow(
      ObjectInspector objectInspector, Object value, int rowId);

  public abstract Object convertTimeUnitFromAvro(ObjectInspector objectInspector, Object value);

  public Schema getAvroSchema(ObjectInspector fieldOi, Field bigqueryField) {
    boolean nullable = bigqueryField.getMode() != Field.Mode.REQUIRED;
    if (fieldOi instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) fieldOi;
      ObjectInspector elementOi = loi.getListElementObjectInspector();
      return AvroUtils.nullableAvroSchema(
          Schema.createArray(getAvroSchema(elementOi, bigqueryField)), nullable);
    }
    if (fieldOi instanceof StructObjectInspector) {
      return AvroUtils.nullableAvroSchema(
          AvroUtils.getAvroSchema((StructObjectInspector) fieldOi, bigqueryField.getSubFields()),
          nullable);
    }
    if (fieldOi instanceof MapObjectInspector) {
      // Convert the Map type into a list of key/value records
      MapObjectInspector moi = (MapObjectInspector) fieldOi;
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema.Field keyField =
          new Schema.Field(KeyValueObjectInspector.KEY_FIELD_NAME, keySchema, null, null);
      Schema valueSchema =
          getAvroSchema(
              moi.getMapValueObjectInspector(),
              bigqueryField.getSubFields().get(KeyValueObjectInspector.VALUE_FIELD_NAME));
      Schema.Field valueField =
          new Schema.Field(KeyValueObjectInspector.VALUE_FIELD_NAME, valueSchema, null, null);
      Schema entrySchema =
          Schema.createRecord(
              "map_" + UUID.randomUUID().toString().replace("-", ""), null, null, false);
      entrySchema.setFields(Arrays.asList(keyField, valueField));
      return AvroUtils.nullableAvroSchema(Schema.createArray(entrySchema), nullable);
    }
    if (fieldOi instanceof ByteObjectInspector
        || fieldOi instanceof ShortObjectInspector
        || fieldOi instanceof IntObjectInspector
        || (fieldOi instanceof LongObjectInspector)) {
      return AvroUtils.nullableAvroSchema(Schema.create(Schema.Type.LONG), nullable);
    }
    if (fieldOi instanceof TimestampObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      if (bigqueryField.getType().getStandardType().equals(StandardSQLTypeName.TIMESTAMP)) {
        AvroUtils.addProp(schema, "logicalType", "timestamp-micros");
      } else if (bigqueryField.getType().getStandardType().equals(StandardSQLTypeName.DATETIME)) {
        AvroUtils.addProp(schema, "logicalType", "local-timestamp-micros");
      } else {
        throw new RuntimeException(
            String.format(
                "Unexpected BigQuery type `%s` for field `%s` with Hive type `%s`",
                bigqueryField.getType().getStandardType(),
                bigqueryField.getName(),
                fieldOi.getTypeName()));
      }
      return AvroUtils.nullableAvroSchema(schema, nullable);
    }
    if (fieldOi instanceof DateObjectInspector) {
      Schema schema = Schema.create(Schema.Type.INT);
      AvroUtils.addProp(schema, "logicalType", "date");
      return AvroUtils.nullableAvroSchema(schema, nullable);
    }
    if (fieldOi instanceof FloatObjectInspector || fieldOi instanceof DoubleObjectInspector) {
      return AvroUtils.nullableAvroSchema(Schema.create(Schema.Type.DOUBLE), nullable);
    }
    if (fieldOi instanceof BooleanObjectInspector) {
      return AvroUtils.nullableAvroSchema(Schema.create(Schema.Type.BOOLEAN), nullable);
    }
    if (fieldOi instanceof BinaryObjectInspector) {
      return AvroUtils.nullableAvroSchema(Schema.create(Schema.Type.BYTES), nullable);
    }
    if (fieldOi instanceof HiveCharObjectInspector
        || fieldOi instanceof HiveVarcharObjectInspector
        || fieldOi instanceof StringObjectInspector) {
      return AvroUtils.nullableAvroSchema(Schema.create(Schema.Type.STRING), nullable);
    }
    if (fieldOi instanceof HiveDecimalObjectInspector) {
      HiveDecimalObjectInspector hdoi = (HiveDecimalObjectInspector) fieldOi;
      Schema schema = Schema.create(Schema.Type.BYTES);
      AvroUtils.addProp(schema, "logicalType", "decimal");
      AvroUtils.addProp(schema, "precision", new Integer(hdoi.precision()));
      AvroUtils.addProp(schema, "scale", new Integer(hdoi.scale()));
      return AvroUtils.nullableAvroSchema(schema, nullable);
    }

    String unsupportedCategory;
    if (fieldOi instanceof PrimitiveObjectInspector) {
      unsupportedCategory = ((PrimitiveObjectInspector) fieldOi).getPrimitiveCategory().name();
    } else {
      unsupportedCategory = fieldOi.getCategory().name();
    }

    throw new IllegalStateException("Unexpected type: " + unsupportedCategory);
  }

  /** Converts the Hive UDF to the corresponding BigQuery function */
  public GenericUDF convertUDF(ExprNodeGenericFuncDesc expr, Configuration conf) {
    GenericUDF udf = expr.getGenericUDF();
    if (getIdenticalUDFs().contains(udf.getUdfName())) {
      return udf;
    }
    if ((udf instanceof GenericUDFBridge)
        && getIdenticalBridgeUDFs().contains(((GenericUDFBridge) udf).getUdfClassName())) {
      return udf;
    }
    if (HiveCompat.getInstance().getUDFsRequiringExtraParentheses().contains(udf.getUdfName())) {
      return new BigQueryUDFWrapParentheses(udf);
    }
    if (udf instanceof GenericUDFNvl) {
      return new BigQueryUDFIfNull();
    }
    if (udf instanceof GenericUDFDateDiff) {
      return new BigQueryUDFDateDiff();
    }
    if (udf instanceof GenericUDFDateSub) {
      return new BigQueryUDFDateSub();
    }
    if (udf instanceof GenericUDFDateAdd) {
      return new BigQueryUDFDateAdd();
    }
    if (udf instanceof GenericUDFOPMod) {
      return new BigQueryUDFMod();
    }
    if (udf instanceof GenericUDFDate) {
      return new BigQueryUDFDate();
    }
    if (udf instanceof GenericUDFToDate) {
      return new BigQueryUDFCastDate();
    }
    if (udf instanceof GenericUDFTimestamp) {
      return new BigQueryUDFCastDatetime();
    }
    if (udf instanceof GenericUDFToBinary) {
      return new BigQueryUDFCastBytes();
    }
    if (udf instanceof GenericUDFToVarchar) {
      return new BigQueryUDFCastString();
    }
    if (udf instanceof GenericUDFToChar) {
      return new BigQueryUDFCastString();
    }
    if (udf instanceof GenericUDFToDecimal) {
      return new BigQueryUDFCastDecimal();
    }
    if (udf instanceof GenericUDFBridge) {
      String fullClassName = ((GenericUDFBridge) udf).getUdfClassName();
      if (fullClassName.startsWith(hiveUDFPackage)) {
        String className = fullClassName.substring(hiveUDFPackageLength);
        switch (className) {
          case "UDFWeekOfYear":
            return new BigQueryUDFWeekOfYear();
          case "UDFDayOfWeek":
            return new BigQueryUDFDayOfWeek();
          case "UDFHex":
            return new BigQueryUDFToHex();
          case "UDFUnhex":
            return new BigQueryUDFFromHex();
          case "UDFOPBitShiftLeft":
            return new BigQueryUDFShiftLeft();
          case "UDFOPBitShiftRight":
            return new BigQueryUDFShiftRight();
          case "UDFToString":
            return new BigQueryUDFCastString();
          case "UDFToLong":
          case "UDFToInteger":
          case "UDFToShort":
          case "UDFToByte":
            return new BigQueryUDFCastInt64();
          case "UDFToBoolean":
            return new BigQueryUDFCastBoolean();
          case "UDFToFloat":
          case "UDFToDouble":
            return new BigQueryUDFCastFloat64();
        }
      }
    }
    String message = "Unsupported UDF or operator: " + udf.getUdfName();
    if (conf.getBoolean(HiveBigQueryConfig.FAIL_ON_UNSUPPORTED_UDFS, false)) {
      throw new IllegalArgumentException(message);
    }
    LOG.info(message);
    return null;
  }

  public Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName>
      getHiveToBqTypeMappings() {
    String cacheKey = "HiveCompatBase.getHiveToBqTypeMappings";
    Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName> cachedResult =
        (Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName>)
            cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName> map = new HashMap<>();
    map.put(PrimitiveObjectInspector.PrimitiveCategory.CHAR, StandardSQLTypeName.STRING);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.VARCHAR, StandardSQLTypeName.STRING);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.STRING, StandardSQLTypeName.STRING);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.BYTE, StandardSQLTypeName.INT64); // Tiny Int
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.SHORT, StandardSQLTypeName.INT64); // Small Int
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.INT, StandardSQLTypeName.INT64); // Regular Int
    map.put(PrimitiveObjectInspector.PrimitiveCategory.LONG, StandardSQLTypeName.INT64); // Big Int
    map.put(PrimitiveObjectInspector.PrimitiveCategory.FLOAT, StandardSQLTypeName.FLOAT64);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, StandardSQLTypeName.FLOAT64);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN, StandardSQLTypeName.BOOL);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.DATE, StandardSQLTypeName.DATE);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, StandardSQLTypeName.DATETIME);
    map.put(PrimitiveObjectInspector.PrimitiveCategory.BINARY, StandardSQLTypeName.BYTES);
    cache.put(cacheKey, map);
    return map;
  }

  public Map<PrimitiveObjectInspector.PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
      getHiveToProtoMappings() {
    String cacheKey = "HiveCompatBase.getHiveToProtoMappings";
    Map<PrimitiveObjectInspector.PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
        cachedResult =
            (Map<
                    PrimitiveObjectInspector.PrimitiveCategory,
                    DescriptorProtos.FieldDescriptorProto.Type>)
                cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    Map<PrimitiveObjectInspector.PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
        map = new HashMap<>();
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.CHAR,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.VARCHAR,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.STRING,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.BYTE,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.SHORT,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.INT,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.LONG,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.FLOAT,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.DOUBLE,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.DECIMAL,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.DATE,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.BINARY,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);
    cache.put(cacheKey, map);
    return map;
  }

  /** List of built-in Hive UDFs and operators that are exactly the same in BigQuery */
  protected List<String> getIdenticalUDFs() {
    String cacheKey = "HiveCompatBase.getIdenticalUDFs";
    List<String> cachedResult = (List<String>) cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    List<String> udfs = new ArrayList<>();
    for (Class udf :
        new Class[] {
          GenericUDFAbs.class,
          GenericUDFGreatest.class,
          GenericUDFLeast.class,
          GenericUDFIf.class,
          GenericUDFTrim.class,
          GenericUDFLTrim.class,
          GenericUDFRTrim.class,
          GenericUDFUpper.class,
          GenericUDFLower.class,
          GenericUDFCeil.class,
          GenericUDFFloor.class,
          GenericUDFCoalesce.class,
          GenericUDFConcat.class,
          GenericUDFOPNot.class,
          GenericUDFOPEqual.class,
          GenericUDFOPNotEqual.class,
          GenericUDFOPGreaterThan.class,
          GenericUDFOPLessThan.class,
          GenericUDFOPEqualOrGreaterThan.class,
          GenericUDFOPEqualOrLessThan.class,
          GenericUDFIn.class,
          GenericUDFBetween.class,
          GenericUDFOPAnd.class,
          GenericUDFOPOr.class,
          GenericUDFOPPlus.class,
          GenericUDFOPMinus.class,
          GenericUDFOPNegative.class,
          GenericUDFOPPositive.class,
          GenericUDFPower.class,
          GenericUDFOPDivide.class,
          GenericUDFOPMultiply.class,
          GenericUDFCbrt.class,
          GenericUDFCase.class
        }) {
      udfs.add(udf.getName());
    }
    cache.put(cacheKey, udfs);
    return udfs;
  }

  public List<String> getIdenticalBridgeUDFs() {
    String cacheKey = "HiveCompatBase.getIdenticalBridgeUDFs";
    List<String> cachedResult = (List<String>) cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    List<String> result = new ArrayList<>();
    for (Class udf :
        new Class[] {
          UDFOPBitAnd.class,
          UDFOPBitOr.class,
          UDFOPBitNot.class,
          UDFOPBitXor.class,
          UDFSqrt.class,
          UDFCos.class,
          UDFSin.class,
          UDFAcos.class,
          UDFAsin.class,
          UDFTan.class,
          UDFAtan.class
        }) {
      result.add(udf.getName());
    }
    cache.put(cacheKey, result);
    return result;
  }

  public List<String> getUDFsRequiringExtraParentheses() {
    String cacheKey = "HiveCompatBase.getUDFsRequiringExtraParentheses";
    List<String> cachedResult = (List<String>) cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    List<String> result = new ArrayList<>();
    for (Class udf :
        new Class[] {
          GenericUDFOPNull.class, GenericUDFOPNotNull.class,
        }) {
      result.add(udf.getName());
    }
    cache.put(cacheKey, result);
    return result;
  }

  /** Format the value of the predicate (i.e. WHERE clause item) to be compatible with BigQuery. */
  public String formatPredicateValue(TypeInfo typeInfo, Object value) {
    if (value == null) {
      return "NULL";
    }
    if (typeInfo.equals(stringTypeInfo)
        || typeInfo instanceof CharTypeInfo
        || typeInfo instanceof VarcharTypeInfo) {
      return "'" + value + "'";
    }
    if (typeInfo.equals(binaryTypeInfo)) {
      byte[] bytes = (byte[]) value;
      return "B'" + new String(bytes) + "'";
    }
    if (typeInfo.equals(dateTypeInfo)) {
      return "DATE'" + value + "'";
    }
    if (typeInfo.equals(timestampTypeInfo)) {
      return "DATETIME'" + value + "'";
    }
    if (typeInfo.equals(intervalDayTimeTypeInfo)) {
      HiveIntervalDayTime intervalDayTime = (HiveIntervalDayTime) value;
      return "INTERVAL '"
          + intervalDayTime.getTotalSeconds()
          + "."
          + (intervalDayTime.getNanos() / 1000)
          + "' SECOND";
    }
    if (SIMPLE_TYPES.contains(typeInfo.getTypeName()) || typeInfo instanceof DecimalTypeInfo) {
      return value.toString();
    }
    throw new RuntimeException("Unsupported predicate type: " + typeInfo.getTypeName());
  }

  public abstract ExprNodeGenericFuncDesc deserializeExpression(String s);
}
