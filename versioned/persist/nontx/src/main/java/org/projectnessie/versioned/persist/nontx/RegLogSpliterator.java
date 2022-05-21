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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.persist.adapter.RefLog;

final class RegLogSpliterator extends AbstractSpliterator<RefLog> {

  private final List<RefLogSplit> splits;

  private final Hash initialHash;
  private boolean initialHashSeen;
  private RefLog initialRefLog;

  RegLogSpliterator(Hash initialHash, Stream<Spliterator<RefLog>> refLogStripeFetcher)
      throws RefLogNotFoundException {
    super(Long.MAX_VALUE, 0);

    this.initialHash = initialHash;
    this.initialHashSeen = initialHash == null;

    splits = refLogStripeFetcher.map(RefLogSplit::new).collect(Collectors.toList());

    // The API for DatabaseAdapter.refLog(Hash) requires to throw a RefLogNotFoundException,
    // if no RefLog for the initial hash exists. This loop tries to find the initial RefLog entry
    // and throw
    AtomicReference<RefLog> initial = new AtomicReference<>();
    while (!initialHashSeen) {
      if (!tryAdvance(initial::set)) {
        break;
      }
    }

    // Throw RefLogNotFoundException, if RefLog with initial hash could not be found.
    if (!initialHashSeen) {
      throw RefLogNotFoundException.forRefLogId(initialHash.asString());
    }
    this.initialRefLog = initial.get();
  }

  @Override
  public boolean tryAdvance(Consumer<? super RefLog> action) {
    if (initialRefLog != null) {
      // This returns the RefLog entry for the initial hash.
      action.accept(initialRefLog);
      initialRefLog = null;
      return true;
    }

    Optional<RefLogSplit> oldest =
        splits.stream()
            .filter(RefLogSplit::hasMore)
            .max(Comparator.comparing(RefLogSplit::operationTime));

    if (!oldest.isPresent()) {
      return false;
    }

    RefLog refLog = oldest.get().pull();
    if (refLog == null) {
      return false;
    }

    if (!initialHashSeen) {
      initialHashSeen = refLog.getRefLogId().equals(initialHash);
    }

    if (initialHashSeen) {
      action.accept(refLog);
    }

    return true;
  }

  private static final class RefLogSplit {

    private final Spliterator<RefLog> source;

    private boolean exhausted;

    private RefLog current;

    private RefLogSplit(Spliterator<RefLog> source) {
      this.source = source;
    }

    private void advance() {
      if (exhausted) {
        return;
      }

      current = null;
      if (!source.tryAdvance(refLog -> current = refLog)) {
        exhausted = true;
      }
    }

    private void maybeAdvance() {
      if (current == null) {
        advance();
      }
    }

    RefLog pull() {
      if (exhausted) {
        return null;
      }

      maybeAdvance();

      RefLog r = current;
      if (r != null) {
        current = null;
      }
      return r;
    }

    long operationTime() {
      maybeAdvance();

      return current != null ? current.getOperationTime() : 0L;
    }

    boolean hasMore() {
      maybeAdvance();

      return current != null;
    }

    @Override
    public String toString() {
      return "RefLogSplit{" + "exhausted=" + exhausted + ", current=" + current + '}';
    }
  }
}
