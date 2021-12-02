/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.gc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

public class Dataset {
  NamedRef currentRef;
  final List<Consumer<DatabaseAdapter>> ops = new ArrayList<>();
  final Set<NamedRef> knownRefs = new HashSet<>();
  final String name;
  final Map<String, Set<Content>> expectedLive = new HashMap<>();
  final Map<String, Set<Content>> expectedExpired = new HashMap<>();
  Function<Reference, Instant> cutOffTimeStampPerRefFunc;
  static final String DEFAULT_BRANCH = "main";
  static final TableCommitMetaStoreWorker STORE_WORKER = new TableCommitMetaStoreWorker();

  Dataset(String name) {
    this.name = name;
    currentRef = BranchName.of(DEFAULT_BRANCH);
  }

  Dataset withCutOffTimeStampPerRefFunc(Function<Reference, Instant> cutOffTimeStampPerRefFunc) {
    this.cutOffTimeStampPerRefFunc = cutOffTimeStampPerRefFunc;
    return this;
  }

  /**
   * Switches to the given branch, creates the branch if necessary from the current branch's HEAD.
   */
  Dataset switchToBranch(String branchName) {
    NamedRef ref = BranchName.of(branchName);
    NamedRef current = this.currentRef;
    this.currentRef = ref;
    if (knownRefs.add(ref)) {
      ops.add(
          adapter -> {
            try {
              adapter.create(
                  ref, adapter.namedRef(current.getName(), GetNamedRefsParams.DEFAULT).getHash());
            } catch (ReferenceAlreadyExistsException e) {
              // Do nothing, just switch the branch
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
    return this;
  }

  /**
   * Starts building a commit with the given timestamp as the system-commit-timestamp recorded in
   * the commit.
   */
  CommitBuilder getCommitBuilderWithCutoffTime(Instant timestamp) {
    return new CommitBuilder(this, currentRef, timestamp);
  }

  Dataset recordCommit(Consumer<DatabaseAdapter> commitProducer) {
    ops.add(commitProducer);
    return this;
  }

  public void applyToAdapter(DatabaseAdapter databaseAdapter) {
    for (Consumer<DatabaseAdapter> op : ops) {
      op.accept(databaseAdapter);
    }
  }

  /**
   * Verifies that the {@link GCResult} contains the expected content-ids and live/non-live
   * content-values.
   */
  public void verify(GCResult<BasicExpiredContentValues> contentValuesPerType) {
    assertThat(contentValuesPerType.getContentValues()).isNotNull();

    Set<Map.Entry<String, BasicExpiredContentValues>> entries =
        contentValuesPerType.getContentValues().entrySet();

    Map<String, Set<Content>> actualLive =
        entries.stream()
            .filter(e -> !e.getValue().getLiveContents().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getLiveContents()));
    compareContents(actualLive, expectedLive);

    Map<String, Set<Content>> actualExpired =
        entries.stream()
            .filter(e -> !e.getValue().getExpiredContents().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getExpiredContents()));
    compareContents(actualExpired, expectedExpired);
  }

  void compareContents(Map<String, Set<Content>> actual, Map<String, Set<Content>> expected) {
    assertThat(actual.size()).isEqualTo(expected.size());
    for (String key : expected.keySet()) {
      List<Content> expectedContents = new ArrayList<>(expected.get(key));
      expectedContents.sort(new ContentComparator());
      List<Content> actualContents = new ArrayList<>(actual.get(key));
      actualContents.sort(new ContentComparator());
      assertThat(expectedContents.size()).isEqualTo(actualContents.size());
      for (int i = 0; i < expectedContents.size(); i++) {
        compareContent(expectedContents.get(i), actualContents.get(i));
      }
    }
  }

  void compareContent(Content actual, Content expected) {
    if (expected.getType().equals(Content.Type.ICEBERG_TABLE)) {
      // exclude global state in comparison as it will change as per the commit and won't be same
      // as original.
      // compare only snapshot id
      assertThat(((IcebergTable) expected).getSnapshotId())
          .isEqualTo(((IcebergTable) actual).getSnapshotId());
    } else {
      assertThat(expected).isEqualTo(actual);
    }
  }

  @Override
  public String toString() {
    return name;
  }

  static class ContentComparator implements Comparator<Content> {
    @Override
    public int compare(Content c1, Content c2) {
      if (c1.getType().equals(Content.Type.ICEBERG_TABLE)) {
        return Long.compare(
            ((IcebergTable) c1).getSnapshotId(), ((IcebergTable) c2).getSnapshotId());
      } else {
        return c1.getId().compareTo(c2.getId());
      }
    }
  }
}
