/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.utils;

import com.google.cloud.bigquery.storage.v1beta2.CivilTimeEncoder;
import java.time.*;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

public class DateTimeUtils {

  public static long getEpochMicrosFromHiveTimestampTZ(TimestampTZ timestampTZ) {
    return timestampTZ.getZonedDateTime().toEpochSecond() * 1_000_000
        + timestampTZ.getZonedDateTime().getNano() / 1_000;
  }

  public static long getEpochMicrosFromHiveTimestamp(Timestamp timestamp) {
    return timestamp.toEpochSecond() * 1_000_000 + timestamp.getNanos() / 1_000;
  }

  public static long getEncodedProtoLongFromHiveTimestamp(Timestamp timestamp) {
    return CivilTimeEncoder.encodePacked64DatetimeMicros(
        org.threeten.bp.LocalDateTime.of(
            timestamp.getYear(),
            timestamp.getMonth(),
            timestamp.getDay(),
            timestamp.getHours(),
            timestamp.getMinutes(),
            timestamp.getSeconds(),
            timestamp.getNanos()));
  }

  public static TimestampTZ getHiveTimestampTZFromUTC(long utc) {
    long seconds = utc / 1_000_000;
    int nanos = (int) (utc % 1_000_000) * 1_000;
    ZonedDateTime zonedDateTime = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("UTC"));
    return new TimestampTZ(zonedDateTime);
  }

  public static Timestamp getHiveTimestampFromLocalDatetime(LocalDateTime localDateTime) {
    return Timestamp.ofEpochSecond(
        localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
  }
}
