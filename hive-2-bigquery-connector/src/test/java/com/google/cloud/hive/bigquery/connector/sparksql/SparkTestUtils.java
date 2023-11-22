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
package com.google.cloud.hive.bigquery.connector.sparksql;

import com.klarna.hiverunner.HiveShell;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

public class SparkTestUtils {

  /**
   * Creates the Metastore derby database on disk instead of in-memory so it can be shared between
   * Hive and Spark
   */
  public static class DerbyDiskDB {
    public String url;
    public Path dir;

    public DerbyDiskDB(HiveShell hive) {
      dir = hive.getBaseDir().resolve("derby_" + UUID.randomUUID());
      url = String.format("jdbc:derby:%s;create=true", dir);
      hive.setHiveConfValue("javax.jdo.option.ConnectionURL", url);
    }

    /**
     * Deletes the locks on the Derby database so Spark can take it over from Hive (or vice versa).
     * Otherwise, you'll run into this exception: "Another instance of Derby may have already booted
     * the database"
     */
    public void releaseLock() {
      try {
        Files.deleteIfExists(dir.resolve("db.lck"));
        Files.deleteIfExists(dir.resolve("dbex.lck"));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static SparkSession getSparkSession(DerbyDiskDB derby, HiveConf hiveConf) {
    derby.releaseLock();
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.sql.defaultUrlStreamHandlerFactory.enabled", "false")
            .set("spark.hadoop.javax.jdo.option.ConnectionURL", derby.url)
            .setMaster("local");
    Properties hiveProps = hiveConf.getAllProperties();
    for (String key : hiveProps.stringPropertyNames()) {
      String value = hiveProps.getProperty(key);
      sparkConf.set(key, value);
    }
    SparkSession spark =
        SparkSession.builder()
            .appName("example")
            .config(sparkConf)
            .enableHiveSupport()
            .getOrCreate();
    return spark;
  }

  /**
   * Converts the given Spark SQL rows to an array of arrays of primitives for easier comparison in
   * tests.
   */
  public static Object[] simplifySparkRows(Row[] rows) {
    Object[][] result = new Object[rows.length][];
    for (int i = 0; i < rows.length; i++) {
      result[i] = new Object[rows[i].size()];
      for (int j = 0; j < rows[i].size(); j++) {
        result[i][j] = rows[i].get(j);
      }
    }
    return result;
  }

  /** Converts a Spark row to a map of primitives. */
  public static Map<String, Object> convertSparkRowToMap(GenericRowWithSchema row) {
    StructType schema = row.schema();
    String[] fieldNames = schema.fieldNames();
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < fieldNames.length; i++) {
      String fieldName = fieldNames[i];
      DataType fieldType = schema.fields()[i].dataType();
      Object value = row.get(i);
      if (value == null) {
        map.put(fieldName, null);
      } else if (fieldType instanceof StructType) {
        map.put(fieldName, convertSparkRowToMap((GenericRowWithSchema) value));
      } else if (fieldType instanceof ArrayType) {
        List<Object> list = new ArrayList<>();
        for (Object element : (Row[]) value) {
          if (element instanceof GenericRowWithSchema) {
            list.add(convertSparkRowToMap((GenericRowWithSchema) element));
          } else {
            list.add(element);
          }
        }
        map.put(fieldName, list);
      } else {
        map.put(fieldName, value);
      }
    }
    return map;
  }

  /** Converts a Spark array to a simple array of primitives. */
  public static Object[] convertSparkArray(WrappedArray wrappedArray) {
    List<Object> list = new ArrayList<>();
    scala.collection.Iterator iterator = wrappedArray.iterator();
    while (iterator.hasNext()) {
      Object value = iterator.next();
      if (value instanceof GenericRowWithSchema) {
        list.add(convertSparkRowToMap((GenericRowWithSchema) value));
      } else if (value instanceof WrappedArray) {
        list.add(convertSparkArray((WrappedArray) value));
      } else {
        list.add(value);
      }
    }
    return list.toArray();
  }
}
