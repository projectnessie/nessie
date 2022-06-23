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
package org.projectnessie.versioned;

import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class Key implements Comparable<Key> {

  public abstract List<String> getElements();

  static ImmutableKey.Builder builder() {

    return ImmutableKey.builder();
  }

  @Override
  public final int compareTo(Key that) {
    List<String> a = this.getElements();
    List<String> b = that.getElements();
    int max = Math.min(a.size(), b.size());
    for (int i = 0; i < max; i++) {
      int cmp = a.get(i).compareTo(b.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }

    return a.size() - b.size();
  }

  @Override
  public int hashCode() {
    int h = 1;
    for (String element : getElements()) {
      h = 31 * h + element.hashCode();
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Key)) {
      return false;
    }

    Key that = (Key) obj;
    List<String> thisElements = this.getElements();
    List<String> thatElements = that.getElements();
    return thisElements.equals(thatElements);
  }

  public static Key of(String... elements) {
    return ImmutableKey.builder().addElements(elements).build();
  }

  public static Key of(List<String> elements) {
    return ImmutableKey.builder().addAllElements(elements).build();
  }

  @Override
  public String toString() {
    return String.join(".", getElements());
  }
}
