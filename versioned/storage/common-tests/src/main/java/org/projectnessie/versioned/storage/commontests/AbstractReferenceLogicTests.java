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
package org.projectnessie.versioned.storage.commontests;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REPO;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.allInternalRefs;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.emptyPagingToken;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.INTERNAL_PREFIX;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.PagingToken;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/** {@link ReferenceLogic} related tests to be run against every {@link Persist} implementation. */
@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public abstract class AbstractReferenceLogicTests {
  private final Class<?> surroundingTestClass;

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  protected AbstractReferenceLogicTests(Class<?> surroundingTestClass) {
    this.surroundingTestClass = surroundingTestClass;
  }

  @ParameterizedTest
  @CsvSource({
    "0, 1", "1, 1", "5, 1", "50, 1", "5, 3", "50, 10",
  })
  public void rewriteCommitLog(int numSourceCommits, int cutoff) throws Exception {
    var referenceLogic = referenceLogic(persist);
    var commitLogic = commitLogic(persist);
    var indexesLogic = indexesLogic(persist);

    var commits = new ArrayList<CommitObj>();
    var head = EMPTY_OBJ_ID;
    for (int i = 0; i < numSourceCommits; i++) {
      var commit =
          commitLogic.doCommit(
              newCommitBuilder()
                  .message("commit #" + i)
                  .parentCommitId(head)
                  .headers(newCommitHeaders().add("random", UUID.randomUUID().toString()).build())
                  .addAdds(
                      commitAdd(key("key", "num-" + i), 1, EMPTY_OBJ_ID, null, UUID.randomUUID()))
                  .build(),
              List.of());
      commits.add(commit);
      soft.assertThat(commit.seq()).isEqualTo(1 + i);
      head = commit.id();
    }

    var reference =
        referenceLogic.createReference("rewrite-commits-" + numSourceCommits, head, null);

    var expectedCommitSeq = new AtomicInteger(numSourceCommits);

    var notRewritten =
        referenceLogic.rewriteCommitLog(
            reference,
            (num, commit) -> {
              soft.assertThat(commit.seq()).isEqualTo(expectedCommitSeq.getAndDecrement());
              soft.assertThat(num - 1).isEqualTo(numSourceCommits - commit.seq());
              soft.assertThat(commit).isEqualTo(commits.get(numSourceCommits - num));
              return false;
            });

    soft.assertThat(notRewritten).isSameAs(reference);

    var rewritten = referenceLogic.rewriteCommitLog(reference, (num, commit) -> num == cutoff);

    if (numSourceCommits == 0 || numSourceCommits == 1) {
      // nothing changed
      soft.assertThat(rewritten).isSameAs(reference);
    } else {
      soft.assertThat(rewritten.pointer()).isNotEqualTo(reference.pointer());

      var newCommits = newArrayList(commitLogic.commitLog(commitLogQuery(rewritten.pointer())));
      soft.assertThat(newCommits).hasSize(cutoff);

      var newHead = commitLogic.fetchCommit(rewritten.pointer());

      if (cutoff > 1) {
        for (int i = 0; i < cutoff - 1; i++) {
          var newCommit = newCommits.get(i);
          var oldCommit = commits.get(numSourceCommits - i - 1);

          soft.assertThat(newCommit.directParent()).isEqualTo(newCommits.get(i + 1).id());
          soft.assertThat(newCommit.seq()).isEqualTo(cutoff - i);

          var newContents =
              newArrayList(indexesLogic.incrementalIndexFromCommit(newCommit)).stream()
                  .map(StoreIndexElement::content)
                  .collect(Collectors.toList());
          var oldContents =
              newArrayList(indexesLogic.incrementalIndexFromCommit(oldCommit)).stream()
                  .map(StoreIndexElement::content)
                  .collect(Collectors.toList());

          soft.assertThat(newContents).isEqualTo(oldContents);
        }

        var numNonIncremetalOps =
            newArrayList(indexesLogic.incrementalIndexFromCommit(newHead)).stream()
                .filter(c -> c.content().action().currentCommit())
                .count();
        soft.assertThat(numNonIncremetalOps).isEqualTo(1);
        soft.assertThat(newHead.seq()).isEqualTo(cutoff);
        soft.assertThat(newHead.directParent()).isNotEqualTo(EMPTY_OBJ_ID);
        soft.assertThat(newHead.tail()).hasSize(cutoff);
        soft.assertThat(newHead.tail().get(cutoff - 1)).isEqualTo(EMPTY_OBJ_ID);
      } else {
        var numNonIncremetalOps =
            newArrayList(indexesLogic.incrementalIndexFromCommit(newHead)).stream()
                .filter(c -> c.content().action().currentCommit())
                .count();
        soft.assertThat(numNonIncremetalOps).isEqualTo(numSourceCommits);
        soft.assertThat(newHead.seq()).isEqualTo(1);
        soft.assertThat(newHead.directParent()).isEqualTo(EMPTY_OBJ_ID);
        soft.assertThat(newHead.tail()).containsExactly(EMPTY_OBJ_ID);
      }

      soft.assertThat(indexesLogic.buildCompleteIndexOrEmpty(newHead).asKeyList())
          .containsExactlyInAnyOrderElementsOf(
              IntStream.range(0, numSourceCommits)
                  .mapToObj(i -> key("key", "num-" + i))
                  .collect(Collectors.toList()));
    }
  }

  @SuppressWarnings("BusyWait")
  @RepeatedTest(2)
  public void refCreationDeletionWithConcurrentRefsListing() throws Exception {
    assumeThat(surroundingTestClass.getSimpleName())
        .isNotIn(
            // Fails due to "write too old" errors, need to implement logic "everywhere" to handle
            // `DatabaseSpecific.isRetryTransaction()`.
            "ITCockroachDBPersist",
            // Fails due to driver timeouts and LWT issues with Apache Cassandra.
            "ITCassandraPersist");

    IntFunction<String> refName = i -> "refs/heads/branch-" + i + "-" + (i & 7);

    int num = 10;

    List<Integer> indexes = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      indexes.add(i);
    }
    ReferenceLogic refLogic = referenceLogic(persist);

    int readerThreads = 1;
    int createDeleteIterations = 2;

    @SuppressWarnings("resource")
    ExecutorService exec = newFixedThreadPool(readerThreads);
    try {
      AtomicBoolean stop = new AtomicBoolean();
      List<Future<?>> tasks =
          IntStream.range(0, readerThreads)
              .mapToObj(
                  x ->
                      exec.submit(
                          () -> {
                            while (!currentThread().isInterrupted() && !stop.get()) {
                              refLogic
                                  .queryReferences(referencesQuery("refs/heads/"))
                                  .forEachRemaining(ref -> {});
                              try {
                                sleep(1);
                              } catch (InterruptedException e) {
                                break;
                              }
                            }
                          }))
              .collect(Collectors.toList());
      try {
        // Following could be in the for-loop below, but want to have different stack traces for the
        // first and following iterations.

        ObjId id = randomObjId();

        for (int i = 0; i < num; i++) {
          refLogic.createReference(refName.apply(i), id, null);
          sleep(1);
        }
        shuffle(indexes);
        for (int i : indexes) {
          refLogic.deleteReference(refName.apply(i), id);
          sleep(1);
        }

        // Repeat with a different ref-pointer

        for (int iter = 0; iter < createDeleteIterations; iter++) {
          id = randomObjId();

          for (int i = 0; i < num; i++) {
            try {
              refLogic.createReference(refName.apply(i), id, null);
            } catch (RefAlreadyExistsException e) {
              throw new RuntimeException("ae " + e.reference() + " for " + id, e);
            }
            sleep(1);
          }
          shuffle(indexes);
          for (int i : indexes) {
            try {
              refLogic.deleteReference(refName.apply(i), id);
            } catch (RefConditionFailedException e) {
              throw new RuntimeException("ae " + e.reference() + " for " + id, e);
            }
            sleep(1);
          }
        }

        stop.set(true);
        for (Future<?> task : tasks) {
          task.get();
        }
      } finally {
        tasks.forEach(t -> t.cancel(true));
      }

    } finally {
      exec.shutdown();
    }
  }

  @Test
  public void internalReferencesNotVisible() {
    ReferenceLogic refLogic = referenceLogic(persist);

    String[] intNames = allInternalRefs().stream().map(InternalRef::name).toArray(String[]::new);

    soft.assertThat(persist.fetchReferences(intNames))
        .hasSize(intNames.length)
        .doesNotContainNull();

    soft.assertThat(refLogic.getReferences(asList(intNames)))
        .hasSize(intNames.length)
        .containsOnlyNulls();
  }

  @Test
  public void internalRefsCannotBeManaged() {
    ReferenceLogic refLogic = referenceLogic(persist);
    ObjId initialPointer = randomObjId();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> refLogic.createReference(INTERNAL_PREFIX, initialPointer, null));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> refLogic.deleteReference(REF_REPO.name(), randomObjId()));

    Reference refRefs = persist.fetchReference(REF_REPO.name());

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> refLogic.assignReference(refRefs, initialPointer));
    soft.assertThat(persist.fetchReference(REF_REPO.name())).isEqualTo(refRefs);
  }

  @Test
  public void createDeleteGoodCase() throws Exception {
    ReferenceLogic refLogic = referenceLogic(persist);
    ObjId initialPointer = randomObjId();
    String refName = "refs/foo/bar";

    ObjId extendedInfoObj = randomObjId();
    Reference reference = refLogic.createReference(refName, initialPointer, extendedInfoObj);
    soft.assertThat(reference)
        .isEqualTo(
            reference(refName, initialPointer, false, reference.createdAtMicros(), extendedInfoObj))
        .extracting(Reference::createdAtMicros, InstanceOfAssertFactories.LONG)
        .isGreaterThan(0L);

    soft.assertThatThrownBy(() -> refLogic.createReference(refName, initialPointer, randomObjId()))
        .isInstanceOf(RefAlreadyExistsException.class);

    soft.assertThat(refLogic.getReferences(singletonList(refName))).containsExactly(reference);

    soft.assertThatCode(() -> refLogic.deleteReference(refName, initialPointer))
        .doesNotThrowAnyException();

    soft.assertThat(refLogic.getReferences(singletonList(refName))).hasSize(1).containsOnlyNulls();

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REPO.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  @Test
  public void assign() throws Exception {
    ReferenceLogic refLogic = referenceLogic(persist);
    Reference ref = reference("foo", randomObjId(), false, 123L, randomObjId());

    persist.addReference(ref);

    ObjId to = objIdFromString("1234");
    Reference assigned = refLogic.assignReference(ref, to);
    Reference refTo =
        reference(
            "foo",
            to,
            false,
            ref.createdAtMicros(),
            ref.extendedInfoObj(),
            assigned.previousPointers());

    soft.assertThat(assigned)
        .isEqualTo(refTo)
        .extracting(Reference::previousPointers, list(Reference.PreviousPointer.class))
        .extracting(Reference.PreviousPointer::pointer)
        .containsExactly(ref.pointer());

    Reference notExists = reference("not-exists", randomObjId(), false, 0L, null);
    soft.assertThatThrownBy(() -> refLogic.assignReference(notExists, randomObjId()))
        .isInstanceOf(RefNotFoundException.class);

    soft.assertThat(persist.fetchReference("foo")).isEqualTo(refTo);
    soft.assertThat(refLogic.getReferences(singletonList("foo"))).containsExactly(refTo);

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REPO.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  @Test
  public void deleteNonExisting() {
    ReferenceLogic refLogic = referenceLogic(persist);

    Reference notExists = reference("not-exists", randomObjId(), false, 0L, null);
    soft.assertThatThrownBy(() -> refLogic.deleteReference(notExists.name(), notExists.pointer()))
        .isInstanceOf(RefNotFoundException.class);
  }

  @Test
  public void getSingle() throws Exception {
    ReferenceLogic refLogic = referenceLogic(persist);
    ObjId initialPointer = objIdFromString("0000");
    String refName = "refs/foo/bar";

    ObjId extendedInfoObj = randomObjId();
    Reference created = refLogic.createReference(refName, initialPointer, extendedInfoObj);
    soft.assertThat(created)
        .extracting(
            Reference::name, Reference::pointer, Reference::deleted, Reference::extendedInfoObj)
        .containsExactly(refName, initialPointer, false, extendedInfoObj);
    soft.assertThat(refLogic.getReference(refName)).isEqualTo(created);

    String notThere = "refs/heads/not_there";
    soft.assertThatThrownBy(() -> refLogic.getReference(notThere))
        .isInstanceOf(RefNotFoundException.class)
        .hasMessage("Reference " + notThere + " does not exist")
        .asInstanceOf(type(RefNotFoundException.class))
        .extracting(RefNotFoundException::reference)
        .isNull();

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REPO.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  /**
   * Verifies that commit-retries during {@code ReferenceLogicImpl#createReference(String, ObjId)}
   * work.
   */
  @Test
  public void createWithCommitRetry(
      @NessieStoreConfig(name = CONFIG_COMMIT_TIMEOUT_MILLIS, value = "300000") @NessiePersist
          Persist persist)
      throws Exception {
    Persist persistSpy = spy(persist);
    ReferenceLogic refLogic = referenceLogic(persist);

    ObjId initialPointer = randomObjId();
    String refName = "refs/foo/bar";

    singleCommitRetry(persistSpy);

    ObjId extendedInfoObj = randomObjId();
    Reference reference = refLogic.createReference(refName, initialPointer, extendedInfoObj);
    soft.assertThat(reference)
        .isEqualTo(
            reference(
                refName, initialPointer, false, reference.createdAtMicros(), extendedInfoObj));

    soft.assertThat(persist.fetchReference(refName)).isEqualTo(reference);

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REPO.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  @Test
  public void deleteRecoverFromMarkDeletedWithCommitRetry(
      @NessieStoreConfig(name = CONFIG_COMMIT_TIMEOUT_MILLIS, value = "300000") @NessiePersist
          Persist persist)
      throws Exception {
    Persist persistSpy = spy(persist);
    ReferenceLogic refLogic = referenceLogic(persist);
    ObjId initialPointer = randomObjId();
    String refName = "refs/foo/bar";

    ObjId extendedInfoObj = randomObjId();
    Reference reference = refLogic.createReference(refName, initialPointer, extendedInfoObj);
    soft.assertThat(reference)
        .isEqualTo(
            reference(
                refName, initialPointer, false, reference.createdAtMicros(), extendedInfoObj));

    Reference deleted = persistSpy.markReferenceAsDeleted(reference);
    soft.assertThat(persistSpy.fetchReference(refName)).isEqualTo(deleted);
    soft.assertThat(deleted.deleted()).isTrue();

    singleCommitRetry(persistSpy);

    soft.assertThat(refLogic.getReferences(singletonList(refName))).hasSize(1).containsOnlyNulls();

    soft.assertThat(persistSpy.fetchReference(refName)).isNull();

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REPO.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  protected static void singleCommitRetry(Persist persistSpy) {
    // Force one retry-exception during ReferenceLogic.commitCreateReference() commitRetry()-loop
    try {
      doThrow(
              new RefConditionFailedException(
                  reference(REF_REPO.name(), EMPTY_OBJ_ID, false, 123L, null)))
          .doCallRealMethod()
          .when(persistSpy)
          .updateReferencePointer(any(), any());
    } catch (RefNotFoundException | RefConditionFailedException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 3, 10, 99, 100, 101, 305})
  public void referencesQueryAll(int numRefs) {
    ReferenceLogic refLogic = referenceLogic(persist);

    List<Reference> created =
        IntStream.range(0, numRefs)
            .mapToObj(i -> String.format("ref-%10d", i))
            .map(
                n -> {
                  try {
                    return refLogic.createReference(n, randomObjId(), randomObjId());
                  } catch (RefAlreadyExistsException | RetryTimeoutException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    soft.assertThat(created)
        .hasSize(numRefs)
        .isEqualTo(
            refLogic.getReferences(
                IntStream.range(0, numRefs)
                    .mapToObj(i -> String.format("ref-%10d", i))
                    .collect(Collectors.toList())));

    soft.assertThat(newArrayList(refLogic.queryReferences(referencesQuery("ref-"))))
        .containsExactlyInAnyOrderElementsOf(created);

    PagedResult<Reference, String> iter = refLogic.queryReferences(referencesQuery());
    soft.assertThat(iter.tokenForKey(null)).isEqualTo(emptyPagingToken());
    while (iter.hasNext()) {
      String name = iter.next().name();
      PagingToken t = iter.tokenForKey(name);
      soft.assertThat(t).isNotEqualTo(emptyPagingToken());
      soft.assertThat(t.token().toStringUtf8()).isEqualTo(name);

      soft.assertAll();
    }
  }

  /**
   * Exercises a bunch of reference names that can be problematic, if the database uses collators
   * that for example collapse adjacent spaces.
   */
  @Test
  public void referencesListing() throws Exception {
    List<String> refNames =
        Stream.of(
                //
                "a-01",
                "a-1",
                "a-10",
                "a-2",
                "a-20",
                //
                "a01",
                "a1",
                "a10",
                "a2",
                "a20",
                //
                "a-   01",
                "a-    1",
                "a-   10",
                "a-    2",
                "a-   20",
                //
                "ä-   01",
                "ä-    1",
                "ä-   10",
                "ä-    2",
                "ä-   20",
                //
                "b-   01",
                "b-    1",
                "b-   10",
                "b-    2",
                "b-   20",
                //
                "a-     01",
                "a-      1",
                "a-     10",
                "a-      2",
                "a-     20")
            .sorted()
            .collect(Collectors.toList());

    ReferenceLogic refLogic = referenceLogic(persist);

    refLogic.deleteReference("refs/heads/main", EMPTY_OBJ_ID);

    Map<String, Reference> references = new LinkedHashMap<>();
    for (String refName : refNames) {
      references.put(refName, refLogic.createReference(refName, EMPTY_OBJ_ID, randomObjId()));
    }

    ArrayList<Reference> queryResult = newArrayList(refLogic.queryReferences(referencesQuery()));
    soft.assertThat(queryResult).containsExactlyInAnyOrderElementsOf(references.values());

    for (String checkPrefix :
        new String[] {"a", "ä", "a-", "ä-", "a-  ", "ä-  ", "a   ", "a-   ", "a-    "}) {
      queryResult = newArrayList(refLogic.queryReferences(referencesQuery(checkPrefix)));
      soft.assertThat(queryResult)
          .describedAs("prefix: '%s'", checkPrefix)
          .containsExactlyElementsOf(
              references.values().stream()
                  .filter(r -> r.name().startsWith(checkPrefix))
                  .collect(Collectors.toList()));
    }

    soft.assertThat(refLogic.getReferences(refNames))
        .containsExactlyElementsOf(references.values());

    for (String refName : refNames) {
      queryResult = newArrayList(refLogic.queryReferences(referencesQuery(refName)));
      soft.assertThat(queryResult)
          .describedAs("ref: %s", refName)
          .containsExactlyElementsOf(
              references.values().stream()
                  .filter(r -> r.name().startsWith(refName))
                  .collect(Collectors.toList()));

      soft.assertThat(refLogic.getReference(refName)).isEqualTo(references.get(refName));
    }
  }

  @Test
  public void referencesQueryPrefix() {
    ReferenceLogic refLogic = referenceLogic(persist);
    List<Reference> created =
        rangeClosed('A', 'E')
            .mapToObj(c -> ((char) c) + "/")
            .flatMap(pre -> rangeClosed('A', 'E').mapToObj(c -> pre + (char) c))
            .map(
                n -> {
                  try {
                    return refLogic.createReference(n, randomObjId(), randomObjId());
                  } catch (RefAlreadyExistsException | RetryTimeoutException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    soft.assertThat(created).hasSize(5 * 5);

    // Just prefix
    rangeClosed('A', 'E')
        .mapToObj(c -> "" + ((char) c))
        .forEach(
            pre -> {
              List<Reference> result =
                  newArrayList(refLogic.queryReferences(referencesQuery(null, pre, false)));
              soft.assertThat(result).hasSize(5).allMatch(r -> r.name().startsWith(pre + "/"));
            });

    // Begin == page-token
    rangeClosed('A', 'E')
        .mapToObj(c -> "" + ((char) c))
        .forEach(
            pre -> {
              List<Reference> result =
                  newArrayList(
                      refLogic.queryReferences(
                          referencesQuery(pagingToken(copyFromUtf8(pre)), pre, false)));
              soft.assertThat(result).hasSize(5).allMatch(r -> r.name().startsWith(pre + "/"));
            });

    // Begin < page-token
    rangeClosed('A', 'E')
        .mapToObj(c -> "" + ((char) c))
        .forEach(
            pre -> {
              List<Reference> result =
                  newArrayList(
                      refLogic.queryReferences(
                          referencesQuery(pagingToken(copyFromUtf8(pre + "/C")), pre, false)));
              soft.assertThat(result).hasSize(3).allMatch(r -> r.name().startsWith(pre + "/"));
            });

    // dumb page-token (outside prefix)
    rangeClosed('A', 'E')
        .mapToObj(c -> "" + ((char) c))
        .forEach(
            pre -> {
              List<Reference> result =
                  newArrayList(
                      refLogic.queryReferences(
                          referencesQuery(pagingToken(copyFromUtf8("Z")), pre, false)));
              soft.assertThat(result).isEmpty();
            });
  }
}
