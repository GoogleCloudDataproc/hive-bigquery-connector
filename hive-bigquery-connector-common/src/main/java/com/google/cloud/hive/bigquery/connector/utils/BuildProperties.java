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
package com.google.cloud.hive.bigquery.connector.utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

public class BuildProperties {

  static final Properties BUILD_PROPERTIES = loadBuildProperties();

  public static final String CONNECTOR_VERSION = BUILD_PROPERTIES.getProperty("connector.version");

  private static Properties loadBuildProperties() {
    try {
      Properties buildProperties = new Properties();
      buildProperties.load(
          BuildProperties.class.getResourceAsStream("/hive-bigquery-connector.properties"));
      return buildProperties;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
