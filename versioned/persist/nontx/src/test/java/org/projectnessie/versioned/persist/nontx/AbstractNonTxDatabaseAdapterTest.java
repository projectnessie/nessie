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
package org.projectnessie.versioned.persist.nontx;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.NamedReference;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefPointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefType;
import org.projectnessie.versioned.persist.tests.AbstractDatabaseAdapterTest;
import org.projectnessie.versioned.persist.tests.LongerCommitTimeouts;

public abstract class AbstractNonTxDatabaseAdapterTest extends AbstractDatabaseAdapterTest {

  private static Set<String> fetchCurrentReferenceNames(
      NonTransactionalDatabaseAdapter<?> nontx, NonTransactionalOperationContext ctx) {
    List<String> allNames =
        StreamSupport.stream(nontx.fetchReferenceNames(ctx), false)
            .flatMap(names -> names.getRefNamesList().stream())
            .collect(Collectors.toList());
    Set<String> namesSet = new HashSet<>(allNames);
    assertThat(allNames).containsExactlyInAnyOrderElementsOf(namesSet);
    return namesSet;
  }

  @Test
  void namedRefsIndex() {
    NonTransactionalDatabaseAdapter<?> nontx = (NonTransactionalDatabaseAdapter<?>) databaseAdapter;

    IntFunction<String> nameGenerator =
        i ->
            "branch-"
                + i
                + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
                + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
                + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
                + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
                + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn";

    final Set<String> testNames =
        IntStream.range(0, 100).mapToObj(nameGenerator).collect(Collectors.toSet());
    final Set<BranchName> testBranches =
        testNames.stream().map(BranchName::of).collect(Collectors.toSet());
    assertThat(testBranches).hasSize(100);

    final Hash head = nontx.noAncestorHash();
    final RefPointer headRef =
        RefPointer.newBuilder().setType(RefType.Branch).setHash(head.asBytes()).build();

    try (NonTransactionalOperationContext ctx = nontx.borrowConnection()) {
      for (BranchName branchName : testBranches) {
        boolean createSuccess = nontx.createNamedReference(ctx, branchName, head);
        assertThat(createSuccess).isTrue();
      }
      assertThat(fetchCurrentReferenceNames(nontx, ctx)).containsAll(testNames);

      for (BranchName branchName : testBranches) {
        boolean deleteSuccess = nontx.deleteNamedReference(ctx, branchName, headRef);
        assertThat(deleteSuccess).isTrue();
      }
      assertThat(fetchCurrentReferenceNames(nontx, ctx)).doesNotContainAnyElementsOf(testNames);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {10, 100, 200, 1000})
  void migrateNamedReferencesFromGlobalPointer(int numNamedRefs) throws Exception {
    assumeThat(numNamedRefs <= 200 || !(this instanceof LongerCommitTimeouts)).isTrue();

    NonTransactionalDatabaseAdapter<?> nontx = (NonTransactionalDatabaseAdapter<?>) databaseAdapter;
    int numThreads = 3;

    Set<NamedReference> createdRefs = new HashSet<>();

    // Prepare branches in GlobalPointer

    try (NonTransactionalOperationContext ctx = nontx.borrowConnection()) {

      GlobalStatePointer globalPointer = nontx.fetchGlobalPointer(ctx);

      assertThat(globalPointer)
          .extracting(GlobalStatePointer::getNamedReferencesCount)
          .isEqualTo(0);

      GlobalStatePointer.Builder newPointer =
          globalPointer.toBuilder().setGlobalId(randomHash().asBytes());

      RefPointer head = nontx.fetchNamedReference(ctx, "main").getRef();

      IntFunction<String> refName = i -> "ref-" + i;

      for (int i = 0; i < numNamedRefs; i++) {
        NamedReference ref =
            NamedReference.newBuilder()
                .setName(refName.apply(i))
                // set some random commit-ID
                .setRef(head.toBuilder().setHash(randomHash().asBytes()))
                .build();
        newPointer.addNamedReferences(ref);
        createdRefs.add(ref);
      }

      assertThat(nontx.globalPointerCas(ctx, globalPointer, newPointer.build())).isTrue();

      // Let 'numThreads' concurrently try to migrate from global-pointer to
      // one-row-per-named-reference.

      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      try {
        CountDownLatch readyLatch = new CountDownLatch(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);

        List<Future<Object>> futures =
            IntStream.range(0, numThreads)
                .mapToObj(
                    i ->
                        executor.submit(
                            () -> {
                              readyLatch.countDown();
                              Preconditions.checkState(startLatch.await(5, MINUTES));

                              try {
                                nontx.fetchNamedReferences(ctx);
                              } finally {
                                doneLatch.countDown();
                              }
                              return null;
                            }))
                .collect(Collectors.toList());

        Preconditions.checkState(readyLatch.await(5, MINUTES));
        startLatch.countDown();

        Preconditions.checkState(doneLatch.await(20, MINUTES));

        assertThat(futures)
            .allSatisfy(f -> assertThatCode(() -> f.get(1, MINUTES)).doesNotThrowAnyException());
      } finally {
        executor.shutdown();
      }

      // verify that the list of named-refs is empty in global-pointer
      assertThat(nontx.fetchGlobalPointer(ctx).getNamedReferencesList()).isEmpty();

      // verify that fetchNamedReferences() returns all created references
      try (Stream<NamedReference> namedRefs = nontx.fetchNamedReferences(ctx)) {
        assertThat(namedRefs.filter(nr -> !"main".equals(nr.getName())))
            .containsExactlyInAnyOrderElementsOf(createdRefs);
      }

      // verify that fetchNamedReference() return the expected value for all created references
      assertThat(createdRefs)
          .allSatisfy(
              namedRef ->
                  assertThat(nontx.fetchNamedReference(ctx, namedRef.getName()))
                      .isEqualTo(namedRef));
    }
  }
}
