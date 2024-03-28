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

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/** Used to capture logs during integration tests. */
public class TestLogAppender extends AppenderSkeleton {
  private List<LoggingEvent> logs = new ArrayList<>();

  @Override
  protected void append(LoggingEvent loggingEvent) {
    logs.add(loggingEvent);
  }

  public List<LoggingEvent> getLogs() {
    return new ArrayList<>(logs);
  }

  @Override
  public void close() {}

  @Override
  public boolean requiresLayout() {
    return false;
  }

  public void clear() {
    logs = new ArrayList<>();
  }
}
