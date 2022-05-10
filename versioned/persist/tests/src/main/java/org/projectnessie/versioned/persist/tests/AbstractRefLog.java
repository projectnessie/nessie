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
package org.projectnessie.versioned.persist.tests;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractRefLog {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractRefLog(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }
  /** Verify that increasing the number of ref-log stripes works. */
  @Test
  void increaseRefLogSplits(
      @NessieDbAdapter(initializeRepo = false)
          @NessieDbAdapterConfigItem(name = "ref.log.stripes", value = "50")
          DatabaseAdapter moreRefLogStripes)
      throws Exception {
    IntFunction<BranchName> ref = i -> BranchName.of("branch-" + i);

    for (int i = 0; i < 50; i++) {
      databaseAdapter.create(ref.apply(i), databaseAdapter.noAncestorHash());
    }

    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      List<BranchName> all = IntStream.range(0, 50).mapToObj(ref).collect(Collectors.toList());
      assertThat(refs.filter(r -> r.getNamedRef().getName().startsWith("branch-")))
          .map(ReferenceInfo::getNamedRef)
          .containsExactlyInAnyOrderElementsOf(all);
    }

    IntFunction<List<String>> branchNames =
        num ->
            IntStream.range(0, num)
                .mapToObj(ref)
                .map(NamedRef::getName)
                .collect(Collectors.toList());
    Function<Stream<RefLog>, Stream<RefLog>> filterCreateBranches =
        refLog ->
            refLog
                .filter(l -> l.getOperation().equals("CREATE_REFERENCE"))
                .filter(l -> l.getRefName().startsWith("branch-"));

    try (Stream<RefLog> refLog = databaseAdapter.refLog(null)) {
      List<String> all = branchNames.apply(50);
      assertThat(filterCreateBranches.apply(refLog))
          .map(RefLog::getRefName)
          .containsExactlyInAnyOrderElementsOf(all);
    }

    try (Stream<RefLog> refLog = moreRefLogStripes.refLog(null)) {
      List<String> all = branchNames.apply(50);
      assertThat(filterCreateBranches.apply(refLog))
          .map(RefLog::getRefName)
          .containsExactlyInAnyOrderElementsOf(all);
    }

    // add 50 more branches

    for (int i = 50; i < 100; i++) {
      moreRefLogStripes.create(ref.apply(i), moreRefLogStripes.noAncestorHash());
    }

    try (Stream<ReferenceInfo<ByteString>> refs =
        moreRefLogStripes.namedRefs(GetNamedRefsParams.DEFAULT)) {
      List<BranchName> all = IntStream.range(0, 100).mapToObj(ref).collect(Collectors.toList());
      assertThat(refs.filter(r -> r.getNamedRef().getName().startsWith("branch-")))
          .map(ReferenceInfo::getNamedRef)
          .containsExactlyInAnyOrderElementsOf(all);
    }

    try (Stream<RefLog> refLog = moreRefLogStripes.refLog(null)) {
      List<String> all = branchNames.apply(100);
      assertThat(filterCreateBranches.apply(refLog))
          .map(RefLog::getRefName)
          .containsExactlyInAnyOrderElementsOf(all);
    }
  }

  @Test
  void emptyRefLog() throws Exception {
    try (Stream<RefLog> refLog = databaseAdapter.refLog(null)) {
      assertThat(refLog)
          .hasSize(1)
          .first()
          .extracting(
              RefLog::getRefName, RefLog::getOperation, RefLog::getCommitHash, RefLog::getParents)
          .containsExactly(
              "main",
              "CREATE_REFERENCE",
              databaseAdapter.noAncestorHash(),
              singletonList(databaseAdapter.noAncestorHash()));
    }
  }

  @Test
  void nonExitingRefLogEntry() {
    assertThatThrownBy(() -> databaseAdapter.refLog(Hash.of("000000")))
        .isInstanceOf(RefLogNotFoundException.class);
  }
}
