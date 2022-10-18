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
import static org.projectnessie.versioned.storage.common.logic.Logics.setupLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.ObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.time.Instant;
import java.util.EnumSet;
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
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.SetupLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/** {@link SetupLogic} related tests to be run against every {@link Persist} implementation. */
@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class AbstractSetupLogicTests {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist(initializeRepo = false)
  protected Persist persist;

  @Test
  public void repositoryExists() {
    SetupLogic setupLogic = setupLogic(persist);
    soft.assertThat(setupLogic.repositoryExists()).isFalse();

    setupLogic.initialize("foobar");
    soft.assertThat(setupLogic.repositoryExists()).isTrue();

    persist.erase();
    soft.assertThat(setupLogic.repositoryExists()).isFalse();

    try (CloseableIterator<Obj> iter = persist.scanAllObjects(EnumSet.allOf(ObjType.class))) {
      soft.assertThat(iter).isExhausted();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void customRepositoryDescription(boolean createDefaultBranch) throws Exception {
    SetupLogic setupLogic = setupLogic(persist);

    Consumer<RepositoryDescription.Builder> builderConsumer =
        b ->
            b.putProperties("foo", "bar")
                .oldestPossibleCommitTime(Instant.ofEpochSecond(12345))
                .repositoryCreatedTime(Instant.ofEpochSecond(456789));

    RepositoryDescription.Builder repoDescBuilder =
        RepositoryDescription.builder().defaultBranchName("main-branch-foo");
    builderConsumer.accept(repoDescBuilder);

    setupLogic.initialize("main-branch-foo", createDefaultBranch, builderConsumer);

    RepositoryDescription repoDesc = setupLogic.fetchRepositoryDescription();
    soft.assertThat(repoDesc).isEqualTo(repoDescBuilder.build());

    ReferenceLogic refLogic = referenceLogic(persist);
    if (createDefaultBranch) {
      soft.assertThat(refLogic.getReference("refs/heads/main-branch-foo"))
          .isEqualTo(reference("refs/heads/main-branch-foo", EMPTY_OBJ_ID, false));
    } else {
      soft.assertThatThrownBy(() -> refLogic.getReference("refs/heads/main-branch-foo"))
          .isInstanceOf(RefNotFoundException.class);
    }
  }

  @Test
  public void emptyRepositoryDescription() throws Exception {
    SetupLogic setupLogic = setupLogic(persist);
    soft.assertThat(setupLogic.fetchRepositoryDescription()).isNull();

    Reference ref =
        persist.addReference(reference(InternalRef.REF_REPO.name(), EMPTY_OBJ_ID, false));
    soft.assertThat(setupLogic.fetchRepositoryDescription()).isNull();

    ObjId nonExisting = randomObjId();
    ref = persist.updateReferencePointer(ref, nonExisting);
    soft.assertThat(ref.pointer()).isEqualTo(nonExisting);
    soft.assertThat(setupLogic.fetchRepositoryDescription()).isNull();

    CommitLogic commitLogic = commitLogic(persist);
    ObjId tip =
        requireNonNull(
            commitLogic.doCommit(
                CreateCommit.newCommitBuilder()
                    .parentCommitId(EMPTY_OBJ_ID)
                    .headers(EMPTY_COMMIT_HEADERS)
                    .message("no key")
                    .build(),
                emptyList()));
    ref = persist.updateReferencePointer(ref, tip);
    soft.assertThat(ref.pointer()).isEqualTo(tip);
    soft.assertThat(setupLogic.fetchRepositoryDescription()).isNull();
  }

  @Test
  public void defaultRepositoryDescription() {
    SetupLogic setupLogic = setupLogic(persist);

    setupLogic.initialize("main");

    RepositoryDescription repoDesc = requireNonNull(setupLogic.fetchRepositoryDescription());
    soft.assertThat(repoDesc.oldestPossibleCommitTime())
        .isEqualTo(repoDesc.repositoryCreatedTime());
  }

  @ParameterizedTest
  @ValueSource(strings = {"main", "master", "some-other-name"})
  public void internalRefsAndDefaultBranch(String defaultBranchName) {
    SetupLogic setupLogic = setupLogic(persist);
    ReferenceLogic referenceLogic = referenceLogic(persist);

    soft.assertThat(allInternalRefs())
        .allSatisfy(
            intRef ->
                assertThat(intRef)
                    .extracting(InternalRef::name, type(String.class))
                    .extracting(persist::findReference)
                    .isNull());

    String refsHeadsMain = "refs/heads/" + defaultBranchName;

    soft.assertThat(refsHeadsMain)
        .satisfies(
            intRef ->
                assertThat(intRef)
                    .asInstanceOf(type(String.class))
                    .extracting(persist::findReference)
                    .isNull());

    soft.assertThat(referenceLogic.queryReferences(referencesQuery())).isExhausted();

    setupLogic.initialize(defaultBranchName);

    Set<Reference> refsByFind =
        Stream.concat(allInternalRefs().stream().map(InternalRef::name), Stream.of(refsHeadsMain))
            .map(persist::findReference)
            .collect(toSet());
    Set<Reference> refQuery = newHashSet(referenceLogic.queryReferences(referencesQuery()));

    soft.assertThat(allInternalRefs())
        .allSatisfy(
            intRef ->
                assertThat(intRef)
                    .extracting(InternalRef::name, type(String.class))
                    .extracting(persist::findReference)
                    .isNotNull()
                    .extracting(Reference::pointer)
                    .isNotEqualTo(EMPTY_OBJ_ID)
                    .satisfies(
                        id -> {
                          CommitObj commitObj = persist.fetchTypedObj(id, COMMIT, CommitObj.class);
                          assertThat(commitObj)
                              .isNotNull()
                              .extracting(CommitObj::commitType)
                              .isSameAs(CommitType.INTERNAL);
                        }));

    soft.assertThat(refsHeadsMain)
        .satisfies(
            intRef ->
                assertThat(intRef)
                    .asInstanceOf(type(String.class))
                    .extracting(persist::findReference)
                    .isNotNull()
                    .extracting(Reference::pointer)
                    .isEqualTo(EMPTY_OBJ_ID));

    // repeat initialization (no-op)

    setupLogic.initialize(defaultBranchName);

    soft.assertThat(
            Stream.concat(
                    allInternalRefs().stream().map(InternalRef::name), Stream.of(refsHeadsMain))
                .map(persist::findReference))
        .containsExactlyInAnyOrderElementsOf(refsByFind);

    soft.assertThat(newHashSet(referenceLogic.queryReferences(referencesQuery())))
        .isEqualTo(refQuery);
  }
}
