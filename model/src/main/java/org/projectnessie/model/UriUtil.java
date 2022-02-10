/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.model;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

final class UriUtil {

  private UriUtil() {}

  public static final char ZERO_BYTE = '\u0000';
  public static final String ZERO_BYTE_STRING = Character.toString(ZERO_BYTE);

  /**
   * Convert from path encoded string to normal string.
   *
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static List<String> fromPathString(String encoded) {
    return Arrays.stream(encoded.split("\\."))
        .map(x -> x.replace('\u0000', '.'))
        .collect(Collectors.toList());
  }

  /**
   * Convert these elements to a URL encoded path string.
   *
   * @return String encoded for path use.
   */
  public static String toPathString(List<String> elements) {
    return elements.stream().map(x -> x.replace('.', '\u0000')).collect(Collectors.joining("."));
  }
}
