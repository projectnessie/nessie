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
package org.projectnessie.services.impl;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** Helper methods around streaming. */
final class StreamUtil {
  private StreamUtil() {}

  /**
   * A spliterator supporting taking until the predicate condition is met. The element that matches
   * the predicate will be included as well.
   *
   * @param src The source {@link Spliterator}
   * @param predicate Taking elements until this {@link Predicate} is met
   * @param <T> the type of elements returned by this spliterator
   * @return A new {@link Spliterator}
   */
  static <T> Spliterator<T> takeUntilIncl(Spliterator<T> src, Predicate<? super T> predicate) {
    return new Spliterators.AbstractSpliterator<T>(src.estimateSize(), 0) {
      boolean found = false;

      @Override
      public boolean tryAdvance(Consumer<? super T> consumer) {
        if (found) {
          return false;
        }
        return src.tryAdvance(
            elem -> {
              consumer.accept(elem);
              if (predicate.test(elem)) {
                found = true;
              }
            });
      }
    };
  }
}
