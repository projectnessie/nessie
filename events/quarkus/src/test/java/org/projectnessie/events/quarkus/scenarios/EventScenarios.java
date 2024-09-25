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
package org.projectnessie.events.quarkus.scenarios;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.ImmutableCommitResult;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.Result;
import org.projectnessie.versioned.TransplantResult;

@Singleton
public class EventScenarios {

  @Inject Consumer<Result> resultCollector;

  final ContentKey key1 = ContentKey.of("foo.bar.table1");
  final ContentKey key2 = ContentKey.of("foo.bar.table2");
  final IcebergTable table = IcebergTable.of("somewhere", 42, 42, 42, 42, "id");
  final List<Operation> operations = Arrays.asList(Put.of(key1, table), Delete.of(key2));

  final Commit commit =
      ImmutableCommit.builder()
          .parentHash(Hash.of("12345678"))
          .hash(Hash.of("567890ab"))
          .operations(operations)
          .commitMeta(
              org.projectnessie.model.ImmutableCommitMeta.builder()
                  .message("commit1")
                  .committer("Alice")
                  .author("Alice")
                  .authorTime(Instant.ofEpochSecond(1234))
                  .commitTime(Instant.ofEpochSecond(5678))
                  .build())
          .build();

  public void commit() {
    CommitResult result =
        ImmutableCommitResult.builder()
            .targetBranch(BranchName.of("branch1"))
            .commit(commit)
            .build();
    resultCollector.accept(result);
  }

  public void merge() {
    MergeResult result =
        ImmutableMergeResult.builder()
            .sourceRef(BranchName.of("branch1"))
            .sourceHash(Hash.of("11111111"))
            .targetBranch(BranchName.of("branch2"))
            .commonAncestor(Hash.of("12345678"))
            .effectiveTargetHash(Hash.of("567890ab"))
            .resultantTargetHash(Hash.of("90abcedf"))
            .addCreatedCommits(commit)
            .build();
    resultCollector.accept(result);
  }

  public void transplant() {
    TransplantResult result =
        TransplantResult.builder()
            .sourceRef(BranchName.of("branch1"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("567890ab"))
            .resultantTargetHash(Hash.of("90abcedf"))
            .addCreatedCommits(commit)
            .build();
    resultCollector.accept(result);
  }

  public void referenceCreated() {
    ReferenceCreatedResult result =
        ImmutableReferenceCreatedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("12345678"))
            .build();
    resultCollector.accept(result);
  }

  public void referenceDeleted() {
    ReferenceDeletedResult result =
        ImmutableReferenceDeletedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("12345678"))
            .build();
    resultCollector.accept(result);
  }

  public void referenceUpdated() {
    ReferenceAssignedResult result =
        ImmutableReferenceAssignedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .previousHash(Hash.of("12345678"))
            .currentHash(Hash.of("567890ab"))
            .build();
    resultCollector.accept(result);
  }
}
