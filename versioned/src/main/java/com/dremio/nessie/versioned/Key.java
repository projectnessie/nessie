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
package com.dremio.nessie.versioned;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface Key extends Comparable<Key> {

  List<String> getElements();

  static ImmutableKey.Builder builder() {
    return ImmutableKey.builder();
  }

  /**
   * Does a case insensitive comparison by key element.
   */
  @Override
  default int compareTo(Key o) {
    List<String> a = this.getElements();
    List<String> b = o.getElements();
    int max = Math.min(a.size(), b.size());
    for (int i = 0; i < max; i++) {
      String as = a.get(i).toLowerCase();
      String bs = b.get(i).toLowerCase();
      int cmp = as.compareTo(bs);
      if (cmp != 0) {
        return cmp;
      }
    }

    return a.size() - b.size();
  }

}