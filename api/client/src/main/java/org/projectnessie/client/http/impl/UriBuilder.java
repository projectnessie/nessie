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

import static java.util.Objects.requireNonNull;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.projectnessie.client.http.HttpClientException;

/**
 * Construct a URI from base and paths. Adds query parameters, supports templates and handles url
 * encoding of path.
 */
public class UriBuilder {

  private static final Pattern BACKSLASH_PATTERN = Pattern.compile("\\+");
  private final URI baseUri;
  private final StringBuilder uri = new StringBuilder();
  private final StringBuilder query = new StringBuilder();
  private final Map<String, String> templateValues = new HashMap<>();

  public UriBuilder(URI baseUri) {
    this.baseUri = requireNonNull(baseUri, "baseUri is null");
  }

  public UriBuilder path(String path) {
    String trimmedPath = HttpUtils.checkNonNullTrim(path);
    HttpUtils.checkArgument(
        !trimmedPath.isEmpty(), "Path %s must be of length greater than 0", trimmedPath);
    if (uri.length() > 0 && !trimmedPath.startsWith("/")) {
      uri.append('/');
    }
    uri.append(trimmedPath);
    return this;
  }

  public UriBuilder queryParam(String name, String value) {
    if (value == null) {
      return this;
    }

    if (query.length() > 0) {
      query.append('&');
    }

    query
        .append(encode(HttpUtils.checkNonNullTrim(name)))
        .append('=')
        .append(encode(HttpUtils.checkNonNullTrim(value)));

    return this;
  }

  public UriBuilder resolveTemplate(String name, String value) {
    templateValues.put(HttpUtils.checkNonNullTrim(name), HttpUtils.checkNonNullTrim(value));
    return this;
  }

  private static void checkEmpty(Map<String, String> templates, StringBuilder uri) {
    if (!templates.isEmpty()) {
      String keys = String.join(";", templates.keySet());
      throw new HttpClientException(
          String.format(
              "Cannot build uri. Not all template keys (%s) were used in uri %s", keys, uri));
    }
  }

  public URI build() throws HttpClientException {
    StringBuilder uriBuilder = new StringBuilder();
    uriBuilder.append(baseUri);

    if (uri.length() > 0) {

      if ('/' != uriBuilder.charAt(uriBuilder.length() - 1)) {
        uriBuilder.append('/');
      }

      Map<String, String> templates = new HashMap<>(templateValues);

      StringBuilder pathElement = new StringBuilder();
      StringBuilder name = new StringBuilder();
      int l = uri.length();
      for (int i = 0; i < l; i++) {
        char c = uri.charAt(i);
        if (c == '/') {
          if (pathElement.length() > 0) {
            uriBuilder.append(encode(pathElement.toString()));
            pathElement.setLength(0);
            uriBuilder.append('/');
          }
          if ('/' != uriBuilder.charAt(uriBuilder.length() - 1)) {
            uriBuilder.append('/');
          }
        } else if (c == '{') {
          for (i++; i < l; i++) {
            c = uri.charAt(i);
            if (c == '}') {
              break;
            }
            name.append(c);
          }
          String value = templates.remove(name.toString());
          if (value != null) {
            pathElement.append(value);
          }
          name.setLength(0);
        } else {
          pathElement.append(c);
        }
      }

      uriBuilder.append(encode(pathElement.toString()));

      checkEmpty(templates, uri);

      // clean off the last / that the joiner added
      if ('/' == uriBuilder.charAt(uriBuilder.length() - 1)) {
        return URI.create(uriBuilder.subSequence(0, uriBuilder.length() - 1).toString());
      }
    } else {
      checkEmpty(templateValues, uri);
    }

    if (query.length() > 0) {
      uriBuilder.append("?");
      uriBuilder.append(query);
    }

    return URI.create(uriBuilder.toString());
  }

  private static String encode(String s) throws HttpClientException {
    try {
      // URLEncoder encodes space ' ' to + according to how encoding forms should work. When
      // encoding URLs %20 should be used instead.
      return BACKSLASH_PATTERN.matcher(URLEncoder.encode(s, "UTF-8")).replaceAll("%20");
    } catch (UnsupportedEncodingException e) {
      throw new HttpClientException(String.format("Cannot url encode %s", s), e);
    }
  }
}
