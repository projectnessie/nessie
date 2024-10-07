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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.allInternalRefs;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.time.Instant;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.ImmutableRepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/** {@link RepositoryLogic} related tests to be run against every {@link Persist} implementation. */
@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class AbstractRepositoryLogicTests {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist(initializeRepo = false)
  protected Persist persist;

  @Test
  public void repositoryExists() {
    RepositoryLogic repositoryLogic = repositoryLogic(persist);
    soft.assertThat(repositoryLogic.repositoryExists()).isFalse();

    repositoryLogic.initialize("foobar");
    soft.assertThat(repositoryLogic.repositoryExists()).isTrue();

    persist.erase();
    soft.assertThat(repositoryLogic.repositoryExists()).isFalse();

    try (CloseableIterator<Obj> iter = persist.scanAllObjects(Set.of())) {
      soft.assertThat(iter).isExhausted();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void customRepositoryDescription(boolean createDefaultBranch) throws Exception {
    RepositoryLogic repositoryLogic = repositoryLogic(persist);

    Consumer<RepositoryDescription.Builder> builderConsumer =
        b ->
            b.putProperties("foo", "bar")
                .oldestPossibleCommitTime(Instant.ofEpochSecond(12345))
                .repositoryCreatedTime(Instant.ofEpochSecond(456789));

    RepositoryDescription.Builder repoDescBuilder =
        RepositoryDescription.builder().defaultBranchName("main-branch-foo");
    builderConsumer.accept(repoDescBuilder);

    repositoryLogic.initialize("main-branch-foo", createDefaultBranch, builderConsumer);

    RepositoryDescription repoDesc = repositoryLogic.fetchRepositoryDescription();
    soft.assertThat(repoDesc).isEqualTo(repoDescBuilder.build());

    ReferenceLogic refLogic = referenceLogic(persist);
    if (createDefaultBranch) {
      soft.assertThat(refLogic.getReference("refs/heads/main-branch-foo"))
          .extracting(
              Reference::name, Reference::pointer, Reference::deleted, Reference::extendedInfoObj)
          .containsExactly("refs/heads/main-branch-foo", EMPTY_OBJ_ID, false, null);
    } else {
      soft.assertThatThrownBy(() -> refLogic.getReference("refs/heads/main-branch-foo"))
          .isInstanceOf(RefNotFoundException.class);
    }
  }

  @Test
  public void emptyRepositoryDescription() throws Exception {
    RepositoryLogic repositoryLogic = repositoryLogic(persist);
    soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isNull();

    Reference ref =
        persist.addReference(
            reference(InternalRef.REF_REPO.name(), EMPTY_OBJ_ID, false, 42L, null));
    soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isNull();

    ObjId nonExisting = randomObjId();
    ref = persist.updateReferencePointer(ref, nonExisting);
    soft.assertThat(ref.pointer()).isEqualTo(nonExisting);
    soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isNull();

    CommitLogic commitLogic = commitLogic(persist);
    ObjId tip =
        requireNonNull(
                commitLogic.doCommit(
                    CreateCommit.newCommitBuilder()
                        .parentCommitId(EMPTY_OBJ_ID)
                        .headers(EMPTY_COMMIT_HEADERS)
                        .message("no key")
                        .build(),
                    emptyList()))
            .id();
    ref = persist.updateReferencePointer(ref, tip);
    soft.assertThat(ref.pointer()).isEqualTo(tip);
    soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isNull();
  }

  @Test
  public void defaultRepositoryDescription() {
    RepositoryLogic repositoryLogic = repositoryLogic(persist);

    repositoryLogic.initialize("main");

    RepositoryDescription repoDesc = requireNonNull(repositoryLogic.fetchRepositoryDescription());
    soft.assertThat(repoDesc.oldestPossibleCommitTime())
        .isEqualTo(repoDesc.repositoryCreatedTime());
  }

  @ParameterizedTest
  @ValueSource(strings = {"main", "master", "some-other-name"})
  public void internalRefsAndDefaultBranch(String defaultBranchName) {
    RepositoryLogic repositoryLogic = repositoryLogic(persist);
    ReferenceLogic referenceLogic = referenceLogic(persist);

    soft.assertThat(allInternalRefs())
        .allSatisfy(
            intRef ->
                assertThat(intRef)
                    .extracting(InternalRef::name, type(String.class))
                    .extracting(persist::fetchReference)
                    .isNull());

    String refsHeadsMain = "refs/heads/" + defaultBranchName;

    soft.assertThat(refsHeadsMain)
        .satisfies(
            intRef ->
                assertThat(intRef)
                    .asInstanceOf(type(String.class))
                    .extracting(persist::fetchReference)
                    .isNull());

    soft.assertThat(referenceLogic.queryReferences(referencesQuery())).isExhausted();

    repositoryLogic.initialize(defaultBranchName);

    Set<Reference> refsByFind =
        Stream.concat(allInternalRefs().stream().map(InternalRef::name), Stream.of(refsHeadsMain))
            .map(persist::fetchReference)
            .collect(toSet());
    Set<Reference> refQuery = newHashSet(referenceLogic.queryReferences(referencesQuery()));

    soft.assertThat(allInternalRefs())
        .allSatisfy(
            intRef ->
                assertThat(intRef)
                    .extracting(InternalRef::name, type(String.class))
                    .extracting(persist::fetchReference)
                    .isNotNull()
                    .extracting(Reference::pointer)
                    .isNotEqualTo(EMPTY_OBJ_ID)
                    .satisfies(
                        id -> {
                          CommitObj commitObj = persist.fetchTypedObj(id, COMMIT, CommitObj.class);
                          assertThat(commitObj)
                              .extracting(CommitObj::commitType)
                              .isSameAs(CommitType.INTERNAL);
                        }));

    soft.assertThat(refsHeadsMain)
        .satisfies(
            intRef ->
                assertThat(intRef)
                    .asInstanceOf(type(String.class))
                    .extracting(persist::fetchReference)
                    .isNotNull()
                    .extracting(Reference::pointer)
                    .isEqualTo(EMPTY_OBJ_ID));

    // repeat initialization (no-op)

    repositoryLogic.initialize(defaultBranchName);

    soft.assertThat(
            Stream.concat(
                    allInternalRefs().stream().map(InternalRef::name), Stream.of(refsHeadsMain))
                .map(persist::fetchReference))
        .containsExactlyInAnyOrderElementsOf(refsByFind);

    soft.assertThat(newHashSet(referenceLogic.queryReferences(referencesQuery())))
        .isEqualTo(refQuery);
  }

  @Test
  public void updateRepositoryDescription() throws RetryTimeoutException {
    RepositoryLogic repositoryLogic = repositoryLogic(persist);

    repositoryLogic.initialize("main");

    RepositoryDescription initial = requireNonNull(repositoryLogic.fetchRepositoryDescription());

    RepositoryDescription updated =
        RepositoryDescription.builder()
            .putProperties("updated", "true")
            .defaultBranchName("main2")
            // the following attributes are read-only, should not be updated
            .oldestPossibleCommitTime(Instant.ofEpochSecond(12345))
            .repositoryCreatedTime(Instant.ofEpochSecond(456789))
            .build();

    RepositoryDescription previous = repositoryLogic.updateRepositoryDescription(updated);

    soft.assertThat(previous).isEqualTo(initial);
    soft.assertThat(repositoryLogic.fetchRepositoryDescription())
        .isEqualTo(
            ImmutableRepositoryDescription.builder()
                .from(initial)
                .putProperties("updated", "true")
                .defaultBranchName("main2")
                .build());
  }
}
