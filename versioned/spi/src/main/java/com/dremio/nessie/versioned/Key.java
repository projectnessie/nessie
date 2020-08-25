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

import java.text.Collator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable
public abstract class Key implements Comparable<Key> {

  private static ThreadLocal<Collator> COLLATOR = new ThreadLocal<Collator>() {
    @Override
    protected Collator initialValue() {
      Collator c = Collator.getInstance(Locale.US);
      c.setStrength(Collator.PRIMARY);
      return c;
    }
  };

  public abstract List<String> getElements();

  static ImmutableKey.Builder builder() {

    return ImmutableKey.builder();
  }

  /**
   * Does a case insensitive comparison by key element.
   */
  @Override
  public final int compareTo(Key o) {
    Collator collator = COLLATOR.get();
    List<String> a = this.getElements();
    List<String> b = o.getElements();
    int max = Math.min(a.size(), b.size());
    for (int i = 0; i < max; i++) {
      int cmp = collator.compare(a.get(i), b.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }

    return a.size() - b.size();
  }

  @Override
  public int hashCode() {
    final Collator collator = COLLATOR.get();
    return getElements().stream().map(s -> collator.getCollationKey(s)).collect(Collectors.toList()).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Key)) {
      return false;
    }
    return compareTo((Key) obj) == 0;
  }

  public static Key of(String...elements) {
    return ImmutableKey.builder().addElements(elements).build();
  }
}
