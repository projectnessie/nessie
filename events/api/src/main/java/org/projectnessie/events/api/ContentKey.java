/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.events.api;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** Key for a {@link Content} object. */
@Value.Immutable
public interface ContentKey extends Comparable<ContentKey> {

  static ContentKey of(String... elements) {
    return ImmutableContentKey.of(Arrays.asList(elements));
  }

  static ContentKey of(Iterable<String> elements) {
    return ImmutableContentKey.of(elements);
  }

  /** The elements of the key. */
  @Value.Parameter
  List<String> getElements();

  /**
   * The simple name of the content key.
   *
   * <p>The simple name is the last element of the content key. For example, the simple name of
   * content key {@code ["a", "b", "c"]} is {@code "c"}.
   */
  @Value.Lazy
  default String getSimpleName() {
    return getElements().get(getElements().size() - 1);
  }

  /**
   * The full name of the content key.
   *
   * <p>The full name is composed of all the elements of the content key, separated by dots. For
   * example, the full name of the content key {@code ["a", "b", "c"]} is {@code "a.b.c"}.
   *
   * <p>When an element contains a dot or the NUL character (unicode {@code U+0000}), it is replaced
   * by the unicode character {@code U+001D}.
   */
  @Value.Lazy
  default String getName() {
    // see org.projectnessie.model.Util
    return getElements().stream()
        .map(element -> element.replace('.', '\u001D').replace('\u0000', '\u001D'))
        .collect(Collectors.joining("."));
  }

  /** Returns the parent key of this content key. */
  @Value.Lazy
  default Optional<ContentKey> getParent() {
    List<String> elements = getElements();
    if (elements.size() <= 1) {
      return Optional.empty();
    }
    return Optional.of(ContentKey.of(elements.subList(0, elements.size() - 1)));
  }

  @Override
  default int compareTo(ContentKey that) {
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
}
