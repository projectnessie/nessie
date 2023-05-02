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
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

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

  /** Validates that the split ref-log-heads works over multiple pages. */
  @Test
  void splitRefLog() throws Exception {
    IntFunction<NamedRef> refGen =
        i -> {
          String name = "splitRefLogTest-" + i;
          return (i & 1) == 1 ? TagName.of(name) : BranchName.of(name);
        };

    Map<NamedRef, List<Tuple>> refLogOpsPerRef = new HashMap<>();

    for (int i = 0; i < 50; i++) {
      NamedRef ref = refGen.apply(i);

      assertThat(databaseAdapter.create(ref, databaseAdapter.noAncestorHash()))
          .isEqualTo(databaseAdapter.noAncestorHash());

      refLogOpsPerRef
          .computeIfAbsent(ref, x -> new ArrayList<>())
          .add(tuple("CREATE_REFERENCE", databaseAdapter.noAncestorHash()));
    }

    // add 50 commits to every branch (crossing the number of parents per commit log entry + ref log
    // entry)
    for (int commit = 0; commit < 50; commit++) {
      for (int i = 0; i < 50; i++) {
        NamedRef ref = refGen.apply(i);
        if (ref instanceof BranchName) {
          databaseAdapter.commit(
              ImmutableCommitParams.builder()
                  .toBranch((BranchName) ref)
                  .commitMetaSerialized(ByteString.copyFromUtf8("foo on " + ref.getName()))
                  .addPuts(
                      KeyWithBytes.of(
                          ContentKey.of("table-" + commit),
                          ContentId.of("c" + commit),
                          (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
                          DefaultStoreWorker.instance()
                              .toStoreOnReferenceState(OnRefOnly.newOnRef("c" + commit))))
                  .build());
        }
      }
    }

    // drop every 3rd reference
    for (int i = 2; i < 50; i += 3) {
      NamedRef ref = refGen.apply(i);
      ReferenceInfo<ByteString> refInfo =
          databaseAdapter.namedRef(ref.getName(), GetNamedRefsParams.DEFAULT);
      databaseAdapter.delete(ref, Optional.empty());
      assertThatThrownBy(() -> databaseAdapter.namedRef(ref.getName(), GetNamedRefsParams.DEFAULT))
          .isInstanceOf(ReferenceNotFoundException.class);

      refLogOpsPerRef
          .computeIfAbsent(ref, x -> new ArrayList<>())
          .add(tuple("DELETE_REFERENCE", refInfo.getHash()));
    }

    // Verify that the CREATE_REFERENCE + DROP_REFERENCE + COMMIT reflog entries exist and are in
    // the right order -> DROP_REFERENCE appear before CREATE_REFERENCE.
    try (Stream<RefLog> refLog = databaseAdapter.refLog(null)) {
      refLog
          .filter(l -> l.getRefName().startsWith("splitRefLogTest-"))
          .forEach(
              l -> {
                NamedRef ref =
                    "Branch".equals(l.getRefType())
                        ? BranchName.of(l.getRefName())
                        : TagName.of(l.getRefName());
                List<Tuple> refOps = refLogOpsPerRef.get(ref);
                assertThat(refOps)
                    .describedAs("RefLog operations %s for %s", refOps, l)
                    .isNotNull()
                    .last()
                    .isEqualTo(tuple(l.getOperation(), l.getCommitHash()));
                refOps.remove(refOps.size() - 1);
              });
    }
    assertThat(refLogOpsPerRef).allSatisfy((ref, ops) -> assertThat(ops).isEmpty());
  }
}
