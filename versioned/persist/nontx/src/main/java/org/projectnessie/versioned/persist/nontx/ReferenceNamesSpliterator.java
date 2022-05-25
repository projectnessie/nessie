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
package org.projectnessie.versioned.persist.nontx;

import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.ReferenceNames;

/**
 * {@link Spliterator} of {@link ReferenceNames} used to return all reference names.
 *
 * <p>Takes a function that takes the required segment number and returns a {@link List} of {@link
 * ReferenceNames}, where the first element is the requested segment plus the prefetched segments,
 * according to {@link NonTransactionalDatabaseAdapterConfig#getReferencesSegmentPrefetch()}. A
 * {@code null} segment in the returned list indicates that there are no more segments.
 */
final class ReferenceNamesSpliterator extends AbstractSpliterator<ReferenceNames> {

  private int segment;
  private int offset;
  private List<ReferenceNames> segments = Collections.emptyList();

  /**
   * Function that takes the segment number of the {@link ReferenceNames} segment to retrieve.
   *
   * <p>The function <em>must</em> return the requested segment in the returned list at index 0, but
   * can return more {@link ReferenceNames} segments (prefetching).
   */
  private final IntFunction<List<ReferenceNames>> fetchReferenceNames;

  ReferenceNamesSpliterator(IntFunction<List<ReferenceNames>> fetchReferenceNames) {
    super(Long.MAX_VALUE, Spliterator.NONNULL);
    this.fetchReferenceNames = fetchReferenceNames;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ReferenceNames> action) {
    if (segment >= offset + segments.size()) {
      offset = segment;
      segments = fetchReferenceNames.apply(offset);
    }

    ReferenceNames referenceNames = segments.get(segment - offset);

    if (referenceNames == null) {
      return false;
    }

    segment++;

    action.accept(referenceNames);
    return true;
  }
}
