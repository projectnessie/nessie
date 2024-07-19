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

final class NumCommitsCutoffPolicy implements CutoffPolicy {

  private final int commits;

  NumCommitsCutoffPolicy(int commits) {
    this.commits = commits;
  }

  @Override
  public boolean isCutoff(@Nonnull Instant commitTime, int numCommits) {
    return numCommits > commits;
  }

  @Override
  public String toString() {
    return "cutoff after " + commits + " commits";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NumCommitsCutoffPolicy)) {
      return false;
    }
    NumCommitsCutoffPolicy that = (NumCommitsCutoffPolicy) o;
    return commits == that.commits;
  }

  @Override
  public int hashCode() {
    return Objects.hash(commits);
  }
}
