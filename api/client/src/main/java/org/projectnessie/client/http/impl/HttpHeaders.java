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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class HttpHeaders {
  private final Map<String, HttpHeader> headers = new LinkedHashMap<>();

  public void put(String name, String value) {
    String key = name.toLowerCase(Locale.ROOT);
    headers.computeIfAbsent(key, x -> new HttpHeader(name)).addValue(value);
  }

  public boolean contains(String name) {
    String key = name.toLowerCase(Locale.ROOT);
    return headers.containsKey(key);
  }

  public void remove(String name) {
    String key = name.toLowerCase(Locale.ROOT);
    headers.remove(key);
  }

  public Iterable<String> getValues(String name) {
    String key = name.toLowerCase(Locale.ROOT);
    HttpHeader h = headers.get(key);
    return h != null ? h.getValues() : Collections.emptyList();
  }

  public Optional<String> getFirstValue(String name) {
    String key = name.toLowerCase(Locale.ROOT);
    HttpHeader h = headers.get(key);
    if (h != null) {
      Iterator<String> it = h.getValues().iterator();
      if (it.hasNext()) {
        return Optional.of(it.next());
      }
    }
    return Optional.empty();
  }

  public Map<String, Iterable<String>> asMap() {
    return headers.values().stream()
        .collect(Collectors.toMap(HttpHeader::getName, HttpHeader::getValues));
  }

  public Collection<HttpHeader> allHeaders() {
    return headers.values();
  }

  public static final class HttpHeader {
    final String name;
    final Set<String> values = new LinkedHashSet<>();

    HttpHeader(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void addValue(String value) {
      this.values.add(value);
    }

    public Iterable<String> getValues() {
      return values;
    }
  }
}
