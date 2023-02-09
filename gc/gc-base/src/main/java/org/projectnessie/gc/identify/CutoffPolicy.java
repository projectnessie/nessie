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

import java.time.Instant;
import javax.annotation.Nonnull;

/**
 * Defines the cutoff point for a reference, which can be a fixed timestamp, a number of commits to
 * retain, a combination of both or none to retain everything.
 */
@FunctionalInterface
public interface CutoffPolicy {

  Instant NO_TIMESTAMP = Instant.MAX;

  CutoffPolicy NONE = new NoneCutoffPolicy();

  /**
   * The timestamp of the "cutoff point" in a reference or {@link #NO_TIMESTAMP} for "no timestamp",
   * never {@code null}.
   */
  default Instant timestamp() {
    return NO_TIMESTAMP;
  }

  boolean isCutoff(@Nonnull @jakarta.annotation.Nonnull Instant commitTime, int numCommits);

  static CutoffPolicy atTimestamp(@Nonnull @jakarta.annotation.Nonnull Instant cutoffTimestamp) {
    return new TimestampCutoffPolicy(cutoffTimestamp);
  }

  static CutoffPolicy numCommits(int commits) {
    return new NumCommitsCutoffPolicy(commits);
  }
}
