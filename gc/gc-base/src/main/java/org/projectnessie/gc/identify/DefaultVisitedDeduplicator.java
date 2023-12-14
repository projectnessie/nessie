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
package org.projectnessie.gc.identify;

import static org.projectnessie.gc.identify.CutoffPolicy.NO_TIMESTAMP;

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.agrona.collections.ObjectHashSet;

/**
 * Helper to avoid duplicate Nessie commit log scan, considering the effective {@code
 * cutoffTimestamp}.
 *
 * <p>Assumes that a commit log scan with cut-off timestamp <em>"A"</em> can be aborted, when a
 * commit log scan with cut-off timestamp <em>"B"</em> has already processed the same commit ID
 * <em>AND</em> {@code B <= A}.
 *
 * <p>Maintains a map of cutoff-timestamp to a set of visited commits. If a given commit has already
 * been visited with a cutoff-timestamp that is equal to or older than the given cutoff-timestamp,
 * live-contents-identification can stop.
 *
 * <p>NOTE: the reason that this deduplicator is not wired up to the Nessie GC tool is that the
 * exact heap pressure needs to be thoroughly determined, because a Java OutOfMemory situation must
 * be avoided.
 */
public final class DefaultVisitedDeduplicator implements VisitedDeduplicator {

  private final Map<Instant, Set<String>> alreadyVisited = new HashMap<>();

  @Override
  public synchronized boolean alreadyVisited(
      @Nonnull Instant cutoffTimestamp, @Nonnull String commitId) {
    if (cutoffTimestamp.equals(NO_TIMESTAMP)) {
      return false;
    }

    for (Entry<Instant, Set<String>> instantSetEntry : alreadyVisited.entrySet()) {
      if (!instantSetEntry.getKey().isAfter(cutoffTimestamp)
          && instantSetEntry.getValue().contains(commitId)) {
        return true;
      }
    }

    Set<String> commits =
        alreadyVisited.computeIfAbsent(cutoffTimestamp, x -> new ObjectHashSet<>());
    return !commits.add(commitId);
  }
}
