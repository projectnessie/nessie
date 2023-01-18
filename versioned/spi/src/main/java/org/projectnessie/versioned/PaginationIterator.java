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
package org.projectnessie.versioned;

import com.google.common.collect.AbstractIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;

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

  abstract class FilteringPaginationIterator<S, T> extends AbstractIterator<T>
      implements PaginationIterator<T> {
    private final Iterator<S> base;

    private final Function<S, T> mapper;

    private final Predicate<S> sourcePredicate;
    private S current;

    public FilteringPaginationIterator(Iterator<S> base, Function<S, T> mapper) {
      this(base, mapper, x -> true);
    }

    public FilteringPaginationIterator(
        Iterator<S> base, Function<S, T> mapper, Predicate<S> sourcePredicate) {
      this.base = base;
      this.mapper = mapper;
      this.sourcePredicate = sourcePredicate;
    }

    @CheckForNull
    @Override
    protected T computeNext() {
      current = null;
      while (true) {
        if (!base.hasNext()) {
          endOfData();
          return null;
        }
        S source = base.next();
        if (!sourcePredicate.test(source)) {
          continue;
        }
        current = source;
        return mapper.apply(source);
      }
    }

    protected S current() {
      return current;
    }

    @Override
    public final String tokenForCurrent() {
      if (current != null) {
        return computeTokenForCurrent();
      }
      return null;
    }

    protected abstract String computeTokenForCurrent();

    @Override
    public void close() {}
  }
}
