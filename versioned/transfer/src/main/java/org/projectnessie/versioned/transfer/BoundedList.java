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
package org.projectnessie.versioned.transfer;

import com.google.common.base.Preconditions;
import java.util.function.Consumer;

/**
 * Keeps track of the last (up to {@code capacity}) elements added to this collection.
 *
 * <p>Operations on this collection type:
 *
 * <ul>
 *   <li>removal of the least recently added element
 *   <li>retrieving the least recently added element
 *   <li>consuming present elements, starting with the most or least recent one
 * </ul>
 */
final class BoundedList<T> {
  private final T[] elements;
  private int size;
  private int nextAdd;

  @SuppressWarnings("unchecked")
  BoundedList(int capacity) {
    elements = (T[]) new Object[capacity];
  }

  void clear() {
    size = 0;
    nextAdd = 0;
  }

  int size() {
    return size;
  }

  T removeLeastRecent() {
    int i = indexOldest();
    size--;
    return elements[i];
  }

  void add(T element) {
    elements[nextAdd++] = element;
    if (nextAdd == elements.length) {
      nextAdd = 0;
    }
    if (size < elements.length) {
      size++;
    }
  }

  T leastRecentElement() {
    return elements[indexOldest()];
  }

  private int indexOldest() {
    Preconditions.checkState(size > 0);
    int numEmpty = elements.length - size;
    int oldest = nextAdd + numEmpty;
    return oldest % elements.length;
  }

  void oldestToNewest(Consumer<T> consumer) {
    if (size == 0) {
      return;
    }
    for (int i = size, p = indexOldest(); i > 0; i--, p++) {
      if (p == elements.length) {
        p = 0;
      }
      consumer.accept(elements[p]);
    }
  }

  void newestToOldest(Consumer<T> consumer) {
    for (int i = size, p = nextAdd - 1; i > 0; i--, p--) {
      if (p < 0) {
        p = elements.length - 1;
      }
      consumer.accept(elements[p]);
    }
  }

  BoundedList<T> copy() {
    BoundedList<T> copy = new BoundedList<>(elements.length);
    copy.nextAdd = nextAdd;
    System.arraycopy(elements, 0, copy.elements, 0, elements.length);
    copy.size = size;
    return copy;
  }

  boolean isEmpty() {
    return size == 0;
  }

  @Override
  public String toString() {
    return "BoundedList[size=" + size + ";capacity=" + elements.length + ']';
  }
}
