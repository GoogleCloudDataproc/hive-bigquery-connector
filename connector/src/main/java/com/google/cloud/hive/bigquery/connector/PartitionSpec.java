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
package com.google.cloud.hive.bigquery.connector;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class PartitionSpec {

  private final String name;
  private final TypeInfo type;
  private String value;
  // TODO: Add comment/description?

  public PartitionSpec(String name, String type) {
    this.name = name;
    this.type = TypeInfoUtils.getTypeInfosFromTypeString(type).get(0);
  }

  public PartitionSpec(String name, String type, String value) {
    this.name = name;
    this.type = TypeInfoUtils.getTypeInfosFromTypeString(type).get(0);
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public TypeInfo getType() {
    return type;
  }

  public String getStaticValue() {
    return value;
  }

  public void setStaticValue(String value) {
    this.value = value;
  }

  public static PartitionSpec getFromJobDetails(JobDetails jobDetails) {
    PartitionSpec partition = null;
    String partitionName =
        jobDetails
            .getTableProperties()
            .getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS);
    if (partitionName != null) {
      String partitionType =
          jobDetails
              .getTableProperties()
              .getProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES);
      partition = new PartitionSpec(partitionName, partitionType);
      if (jobDetails.getOutputPartitionValues() != null) {
        String partitionValue = jobDetails.getOutputPartitionValues().get(partitionName);
        partition.setStaticValue(partitionValue);
      }
    }
    return partition;
  }
}
