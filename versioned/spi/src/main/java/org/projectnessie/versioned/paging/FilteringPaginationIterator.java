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

import com.google.common.collect.AbstractIterator;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class FilteringPaginationIterator<S, T> extends AbstractIterator<T>
    implements PaginationIterator<T> {

  private final Iterator<S> base;

  private final Function<S, T> mapper;

  private final Predicate<S> sourcePredicate;
  private final Predicate<S> stopPredicate;
  private S current;

  public FilteringPaginationIterator(Iterator<S> base, Function<S, T> mapper) {
    this(base, mapper, x -> true);
  }

  public FilteringPaginationIterator(
      Iterator<S> base, Function<S, T> mapper, Predicate<S> sourcePredicate) {
    this(base, mapper, sourcePredicate, x -> false);
  }

  public FilteringPaginationIterator(
      Iterator<S> base,
      Function<S, T> mapper,
      Predicate<S> sourcePredicate,
      Predicate<S> stopPredicate) {
    this.base = base;
    this.mapper = mapper;
    this.sourcePredicate = sourcePredicate;
    this.stopPredicate = stopPredicate;
  }

  @Override
  protected T computeNext() {
    current = null;
    while (true) {
      if (!base.hasNext()) {
        endOfData();
        return null;
      }
      S source = base.next();
      if (stopPredicate.test(source)) {
        endOfData();
        return null;
      }
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
