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
package org.projectnessie.versioned.storage.common.objtypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

final class CommitHeadersImpl implements CommitHeaders {
  private final Map<String, List<String>> map;

  private CommitHeadersImpl(Map<String, List<String>> map) {
    this.map = map;
  }

  static CommitHeaders.Builder builder() {
    return new BuilderImpl(new HashMap<>());
  }

  static final class BuilderImpl implements CommitHeaders.Builder {
    private final Map<String, List<String>> map;

    public BuilderImpl(Map<String, List<String>> map) {
      this.map = map;
    }

    @Override
    public CommitHeaders.Builder add(String name, String value) {
      nameList(name).add(value);
      return this;
    }

    @Override
    public CommitHeaders.Builder from(CommitHeaders headers) {
      CommitHeadersImpl h = (CommitHeadersImpl) headers;
      for (Entry<String, List<String>> e : h.map.entrySet()) {
        nameList(e.getKey()).addAll(e.getValue());
      }
      return this;
    }

    private List<String> nameList(String name) {
      return map.computeIfAbsent(lowerCase(name), x -> new ArrayList<>());
    }

    @Override
    public CommitHeaders build() {
      return new CommitHeadersImpl(new HashMap<>(map));
    }
  }

  @Override
  public String getFirst(String name) {
    List<String> el = map.get(lowerCase(name));
    if (el == null) {
      return null;
    }
    // index #0 is the capital attribute name
    return el.get(0);
  }

  @Override
  public List<String> getAll(String name) {
    List<String> el = map.get(lowerCase(name));
    if (el == null) {
      return null;
    }
    return Collections.unmodifiableList(el);
  }

  @Override
  public Set<String> keySet() {
    return Collections.unmodifiableSet(map.keySet());
  }

  static String lowerCase(String name) {
    return name.toLowerCase(Locale.ROOT);
  }

  @Override
  public int hashCode() {
    return map.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CommitHeadersImpl)) {
      return false;
    }
    CommitHeadersImpl o = (CommitHeadersImpl) obj;
    return map.equals(o.map);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CommitHeaders{");
    boolean first = true;
    for (Entry<String, List<String>> e : map.entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(e.getKey()).append("=[");
      boolean f = true;
      for (String v : e.getValue()) {
        if (f) {
          f = false;
        } else {
          sb.append(",");
        }
        sb.append(v);
      }
      sb.append(']');
    }
    return sb.append('}').toString();
  }
}
