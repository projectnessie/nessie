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
package org.projectnessie.versioned.paging;

import java.util.Iterator;
import java.util.NoSuchElementException;

public interface PaginationIterator<T> extends Iterator<T>, AutoCloseable {

  String tokenForCurrent();

  @Override
  void close();

  @SuppressWarnings("unchecked")
  static <T> PaginationIterator<T> of(T... values) {
    return new PaginationIterator<T>() {
      int idx = 0;

      @Override
      public String tokenForCurrent() {
        return null;
      }

      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return idx < values.length;
      }

      @Override
      public T next() {
        return values[idx++];
      }
    };
  }

  static <T> PaginationIterator<T> empty() {
    return new PaginationIterator<T>() {
      @Override
      public String tokenForCurrent() {
        return null;
      }

      @Override
      public void close() {}

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        throw new NoSuchElementException();
      }
    };
  }
}
