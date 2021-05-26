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

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Construct a URI from base and paths. Adds query parameters, supports templates and handles url
 * encoding of path.
 */
class UriBuilder {

  private final URI baseUri;
  private final StringBuilder uri = new StringBuilder();
  private final StringBuilder query = new StringBuilder();
  private final Map<String, String> templateValues = new HashMap<>();

  UriBuilder(URI baseUri) {
    this.baseUri = Objects.requireNonNull(baseUri);
  }

  UriBuilder path(String path) {
    if (uri.length() > 0) {
      uri.append('/');
    }
    String trimmedPath = HttpUtils.checkNonNullTrim(path);
    HttpUtils.checkArgument(
        trimmedPath.length() > 0, "Path %s must be of length greater than 0", trimmedPath);
    uri.append(trimmedPath);
    return this;
  }

  UriBuilder queryParam(String name, String value) {
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

  UriBuilder resolveTemplate(String name, String value) {
    templateValues.put(
        String.format("{%s}", HttpUtils.checkNonNullTrim(name)), HttpUtils.checkNonNullTrim(value));
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

  @SuppressWarnings("ResultOfMethodCallIgnored")
  URI build() throws HttpClientException {
    StringBuilder uriBuilder = new StringBuilder();
    uriBuilder.append(baseUri);

    if ('/' != uriBuilder.charAt(uriBuilder.length() - 1)) {
      uriBuilder.append('/');
    }

    if (uri.length() > 0) {
      Map<String, String> templates = new HashMap<>(templateValues);
      // important note: this assumes an entire path component is the template. So /a/{b}/c will
      // work but /a/b{b}/c will not.
      Arrays.stream(uri.toString().split("/"))
          .map(p -> encode((templates.containsKey(p)) ? templates.remove(p) : p))
          .forEach(
              x -> {
                if ('/' != uriBuilder.charAt(uriBuilder.length() - 1)) {
                  uriBuilder.append('/');
                }
                uriBuilder.append(x);
              });

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
      return URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20");
    } catch (UnsupportedEncodingException e) {
      throw new HttpClientException(String.format("Cannot url encode %s", s), e);
    }
  }
}
