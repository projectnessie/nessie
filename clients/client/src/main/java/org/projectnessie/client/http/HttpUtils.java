/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.http;

import java.util.Objects;

public final class HttpUtils {

  public static final String GZIP = "gzip";
  public static final String DEFLATE = "deflate";
  public static final String ACCEPT_ENCODING = GZIP + ";q=1.0, " + DEFLATE + ";q=0.9";
  public static final String HEADER_ACCEPT = "Accept";
  public static final String HEADER_ACCEPT_ENCODING = "Accept-Encoding";
  public static final String HEADER_CONTENT_ENCODING = "Content-Encoding";
  public static final String HEADER_CONTENT_TYPE = "Content-Type";

  private HttpUtils() {}

  /**
   * Check if argument is false. If false throw formatted error.
   *
   * @param expression expression which should be true
   * @param msg message with formatting
   * @param vars string format args
   */
  public static void checkArgument(boolean expression, String msg, Object... vars) {
    if (!expression) {
      throw new IllegalArgumentException(String.format(msg, vars));
    }
  }

  /**
   * check if base is null and if not trim any whitespace.
   *
   * @param str string to check if null
   * @return trimmed str
   */
  public static String checkNonNullTrim(String str) {
    Objects.requireNonNull(str);
    return str.trim();
  }
}
