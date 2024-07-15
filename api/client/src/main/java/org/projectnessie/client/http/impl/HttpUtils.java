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
package org.projectnessie.client.http.impl;

import com.google.errorprone.annotations.FormatMethod;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;

/**
 * Utility methods for HTTP clients.
 *
 * <p>This class should only be used by Nessie HTTP client implementations.
 */
public final class HttpUtils {

  public static final String GZIP = "gzip";
  public static final String DEFLATE = "deflate";
  public static final String GZIP_DEFLATE = GZIP + ";q=1.0, " + DEFLATE + ";q=0.9";
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
  @FormatMethod
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
    return str.trim();
  }

  public static void applyHeaders(HttpHeaders headers, URLConnection con) {
    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        con.addRequestProperty(header.getName(), value);
      }
    }
  }

  /**
   * Parse query parameters from URI. This method cannot handle multiple values for the same
   * parameter.
   *
   * @param query They query string to parse
   * @return map of query parameters
   */
  public static Map<String, String> parseQueryString(String query) {
    if (query == null) {
      throw new IllegalArgumentException("Missing query string");
    }
    Map<String, String> params = new HashMap<>();
    String[] pairs = query.split("&");
    for (String pair : pairs) {
      int idx = pair.indexOf("=");
      String name;
      String value;
      try {
        name = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
        value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        // cannot happen with UTF-8
        throw new IllegalStateException(e);
      }
      params.put(name, value);
    }
    return params;
  }

  public static boolean isHttpUri(URI uri) {
    return "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
  }
}
