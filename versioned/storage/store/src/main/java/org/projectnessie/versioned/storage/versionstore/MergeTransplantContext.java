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
package org.projectnessie.versioned.storage.versionstore;

import java.util.List;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;

/** Source commits for merge and transplant operations. */
final class MergeTransplantContext {

  /** Source commits in chronological order, most recent commit last. */
  private final List<CommitObj> sourceCommits;

  /** Parent of the oldest commit. */
  private final CommitObj baseCommit;

  private final CommitObj headCommit;
  private final CommitMeta metadata;
  private final int numCommits;

  MergeTransplantContext(List<CommitObj> sourceCommits, CommitObj baseCommit, CommitMeta metadata) {
    this.sourceCommits = sourceCommits;
    this.baseCommit = baseCommit;
    int sourceCommitCount = sourceCommits.size();
    this.headCommit = sourceCommitCount > 0 ? sourceCommits.get(sourceCommitCount - 1) : null;
    this.metadata = metadata;
    this.numCommits = sourceCommits.size();
  }

  MergeTransplantContext(CommitObj headCommit, CommitObj baseCommit, CommitMeta metadata) {
    this.sourceCommits = null;
    this.baseCommit = baseCommit;
    this.headCommit = headCommit;
    this.metadata = metadata;
    this.numCommits = 0;
  }

  List<CommitObj> sourceCommits() {
    return sourceCommits;
  }

  CommitObj baseCommit() {
    return baseCommit;
  }

  CommitObj headCommit() {
    return headCommit;
  }

  CommitMeta metadata() {
    return metadata;
  }

  int numCommits() {
    return numCommits;
  }
}
