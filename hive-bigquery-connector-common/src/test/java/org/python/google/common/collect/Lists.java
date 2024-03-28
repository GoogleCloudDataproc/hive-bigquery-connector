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
package org.python.google.common.collect;

import java.util.ArrayList;

/**
 * This is a hack to get around a bug in Pig 0.17.0, which uses a wrong import:
 * `org.python.google.common.collect.Lists` instead of `com.google.common.collect.Lists`. This was
 * fixed in trunk but never released in an official version:
 * https://github.com/apache/pig/commit/6dd3ca4deb84edd9edd7765aa1d12f89a31b1283 So we just define
 * this here, so we can run the tests with Pig 0.17.0.
 */
public class Lists {

  public static <E> ArrayList<E> newArrayList(E... elements) {
    return com.google.common.collect.Lists.newArrayList(elements);
  }
}
