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
package com.dremio.nessie.client.http;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Construct a URI from base and paths. Adds query parameters, supports templates and handles url encoding of path.
 */
class UriBuilder {

  private final String base;
  private final List<String> uri = new ArrayList<>();
  private final Map<String, String> query = new HashMap<>();
  private final Map<String, String> templateValues = new HashMap<>();

  UriBuilder(String base) {
    this.base = base;
  }

  UriBuilder path(String path) {
    uri.add(path);
    return this;
  }

  UriBuilder queryParam(String name, String value) {
    query.put(name, value);
    return this;
  }

  UriBuilder resolveTemplate(String name, String value) {
    templateValues.put(String.format("{%s}", name), value);
    return this;
  }

  String build() throws HttpClientException {
    StringBuilder uriBuilder = new StringBuilder();
    List<String> transformedUri = new ArrayList<>();
    transformedUri.add(base);
    for (String s: this.uri) {
      transformedUri.add(template(s));
    }
    if (!templateValues.isEmpty()) {
      String keys = String.join(";", templateValues.keySet());
      throw new IllegalStateException(String.format("Cannot build uri. Not all template keys (%s) were used in uri %s", keys, uri));
    }
    appendTo(uriBuilder, transformedUri.iterator(), "/");

    if (!query.isEmpty()) {
      uriBuilder.append("?");
      List<String> params = new ArrayList<>();
      for (Map.Entry<String, String> kv: query.entrySet()) {
        if (kv.getValue() == null) {
          continue;
        }
        params.add(String.format("%s=%s", encode(kv.getKey()), encode(kv.getValue())));
      }
      appendTo(uriBuilder, params.iterator(), "&");
    }
    return uriBuilder.toString();
  }

  private String template(String input) throws HttpClientException {
    if (!templateValues.isEmpty() && input.contains("{")) {
      return Arrays.stream(input.split("/"))
                   .map(p -> (templateValues.containsKey(p)) ? templateValues.remove(p) : p)
                   .map(UriBuilder::encode)
                   .collect(Collectors.joining("/"));
    } else {
      return encode(input);
    }
  }

  private static String encode(String s) throws HttpClientException {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new HttpClientException(e);
    }
  }

  private static void appendTo(StringBuilder appendable, Iterator<String> parts, String separator) {
    if (parts.hasNext()) {
      appendable.append(parts.next());
      while (parts.hasNext()) {
        appendable.append(separator);
        appendable.append(parts.next());
      }
    }
  }

}
