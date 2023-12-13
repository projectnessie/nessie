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

import jakarta.annotation.Nonnull;
import java.time.Instant;
import java.util.Objects;

final class TimestampCutoffPolicy implements CutoffPolicy {

  private final Instant cutoffTimestamp;

  TimestampCutoffPolicy(Instant cutoffTimestamp) {
    this.cutoffTimestamp = cutoffTimestamp;
  }

  @Override
  public boolean isCutoff(@Nonnull Instant commitTime, int numCommits) {
    return commitTime.compareTo(cutoffTimestamp) < 0L;
  }

  @Override
  public String toString() {
    return "cutoff at timestamp " + cutoffTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampCutoffPolicy)) {
      return false;
    }
    TimestampCutoffPolicy that = (TimestampCutoffPolicy) o;
    return cutoffTimestamp.equals(that.cutoffTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cutoffTimestamp);
  }
}
