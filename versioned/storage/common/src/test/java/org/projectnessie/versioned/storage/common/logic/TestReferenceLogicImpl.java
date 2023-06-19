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
package org.projectnessie.versioned.storage.common.logic;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.spy;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REFS;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferenceLogicImpl.CommitReferenceResult.Kind.ADDED_TO_INDEX;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogicImpl.CommitReferenceResult;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogicImpl.CommitReferenceResult.Kind;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.commontests.AbstractReferenceLogicTests;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;

public class TestReferenceLogicImpl extends AbstractReferenceLogicTests {

  enum CreateFailures {
    AFTER_COMMIT_CREATED
  }

  enum PostActions {
    GET_REFERENCE,
    QUERY_REFERENCES,
    CREATE_REFERENCE_SAME_POINTER,
    CREATE_REFERENCE_OTHER_POINTER,
    REMOVE_REFERENCE_SAME_POINTER,
    REMOVE_REFERENCE_OTHER_POINTER
  }

  static Stream<Arguments> createRecoverScenarios() {
    return Arrays.stream(CreateFailures.values())
        .flatMap(f -> Arrays.stream(PostActions.values()).map(p -> arguments(f, p)));
  }

  public TestReferenceLogicImpl() {
    super(TestReferenceLogicImpl.class);
  }

  @ParameterizedTest
  @MethodSource("createRecoverScenarios")
  public void createRecoverScenarios(CreateFailures createFailure, PostActions postCreateAction)
      throws Exception {
    ReferenceLogicImpl refLogic = new ReferenceLogicImpl(persist);
    ObjId initialPointer = objIdFromString("0000");
    String refName = "refs/foo/bar";
    ObjId extendedInfoObj = objIdFromString("beef");

    Reference created;
    //noinspection SwitchStatementWithTooFewBranches
    switch (createFailure) {
      case AFTER_COMMIT_CREATED:
        CommitReferenceResult commitCreate =
            refLogic.commitCreateReference(refName, initialPointer, extendedInfoObj);
        soft.assertThat(commitCreate.kind).isSameAs(ADDED_TO_INDEX);
        created = commitCreate.reference;
        soft.assertThat(persist.fetchReference(refName)).isNull();
        break;
      default:
        throw new IllegalArgumentException();
    }

    switch (postCreateAction) {
      case GET_REFERENCE:
        soft.assertThat(refLogic.getReferences(singletonList(refName))).containsExactly(created);
        soft.assertThat(persist.fetchReference(refName)).isEqualTo(created);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      case QUERY_REFERENCES:
        soft.assertThat(newArrayList(refLogic.queryReferences(referencesQuery(refName))))
            .containsExactly(created);
        soft.assertThat(persist.fetchReference(refName)).isEqualTo(created);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      case CREATE_REFERENCE_SAME_POINTER:
        soft.assertThatThrownBy(() -> refLogic.createReference(refName, initialPointer, null))
            .isInstanceOf(RefAlreadyExistsException.class);
        soft.assertThat(persist.fetchReference(refName)).isEqualTo(created);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      case CREATE_REFERENCE_OTHER_POINTER:
        soft.assertThatThrownBy(
                () -> refLogic.createReference(refName, objIdFromString("0001"), null))
            .isInstanceOf(RefAlreadyExistsException.class);
        soft.assertThat(persist.fetchReference(refName)).isEqualTo(created);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      case REMOVE_REFERENCE_SAME_POINTER:
        soft.assertThatCode(() -> refLogic.deleteReference(refName, initialPointer))
            .doesNotThrowAnyException();
        soft.assertThat(persist.fetchReference(refName)).isNull();
        soft.assertThat(indexActionExists(refLogic, refName)).isFalse();
        break;
      case REMOVE_REFERENCE_OTHER_POINTER:
        soft.assertThatThrownBy(() -> refLogic.deleteReference(refName, objIdFromString("0001")))
            .isInstanceOf(RefConditionFailedException.class);
        soft.assertThat(persist.fetchReference(refName)).isEqualTo(created);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      default:
        throw new IllegalArgumentException();
    }

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REFS.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  enum DeleteFailures {
    AFTER_MARK_DELETE,
    AFTER_COMMIT_DELETED
  }

  static Stream<Arguments> deleteRecoverScenarios() {
    return Arrays.stream(DeleteFailures.values())
        .flatMap(f -> Arrays.stream(PostActions.values()).map(p -> arguments(f, p)));
  }

  @ParameterizedTest
  @MethodSource("deleteRecoverScenarios")
  public void deleteRecoverScenarios(DeleteFailures deleteFailure, PostActions postAction)
      throws Exception {
    ReferenceLogicImpl refLogic = new ReferenceLogicImpl(persist);
    ObjId initialPointer = randomObjId();
    String refName = "refs/foo/bar";
    ObjId extendedInfoObj = randomObjId();

    Reference reference = refLogic.createReference(refName, initialPointer, extendedInfoObj);
    soft.assertThat(reference)
        .extracting(
            Reference::name, Reference::pointer, Reference::deleted, Reference::extendedInfoObj)
        .containsExactly(refName, initialPointer, false, extendedInfoObj);

    Reference deleted = persist.markReferenceAsDeleted(reference);
    soft.assertThat(persist.fetchReference(refName)).isEqualTo(deleted);
    soft.assertThat(deleted.deleted()).isTrue();

    switch (deleteFailure) {
      case AFTER_MARK_DELETE:
        break;
      case AFTER_COMMIT_DELETED:
        refLogic.commitDeleteReference(deleted, null);
        break;
      default:
        throw new IllegalArgumentException();
    }

    ObjId otherPointer = randomObjId();
    ObjId otherExtendedInfoObj = randomObjId();
    switch (postAction) {
      case GET_REFERENCE:
        soft.assertThat(refLogic.getReferences(singletonList(refName)))
            .hasSize(1)
            .containsOnlyNulls();
        soft.assertThat(persist.fetchReference(refName)).isNull();
        soft.assertThat(indexActionExists(refLogic, refName)).isFalse();
        break;
      case QUERY_REFERENCES:
        soft.assertThat(newArrayList(refLogic.queryReferences(referencesQuery(refName)))).isEmpty();
        soft.assertThat(persist.fetchReference(refName)).isNull();
        soft.assertThat(indexActionExists(refLogic, refName)).isFalse();
        break;
      case CREATE_REFERENCE_SAME_POINTER:
        soft.assertThatCode(
                () -> refLogic.createReference(refName, initialPointer, otherExtendedInfoObj))
            .doesNotThrowAnyException();
        soft.assertThat(persist.fetchReference(refName))
            .extracting(
                Reference::name, Reference::pointer, Reference::deleted, Reference::extendedInfoObj)
            .containsExactly(
                reference.name(), reference.pointer(), reference.deleted(), otherExtendedInfoObj);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      case CREATE_REFERENCE_OTHER_POINTER:
        soft.assertThatCode(
                () -> refLogic.createReference(refName, otherPointer, otherExtendedInfoObj))
            .doesNotThrowAnyException();
        soft.assertThat(persist.fetchReference(refName))
            .extracting(
                Reference::name, Reference::pointer, Reference::deleted, Reference::extendedInfoObj)
            .containsExactly(refName, otherPointer, false, otherExtendedInfoObj);
        soft.assertThat(indexActionExists(refLogic, refName)).isTrue();
        break;
      case REMOVE_REFERENCE_SAME_POINTER:
        soft.assertThatThrownBy(() -> refLogic.deleteReference(refName, initialPointer))
            .isInstanceOf(RefNotFoundException.class);
        soft.assertThat(persist.fetchReference(refName)).isNull();
        soft.assertThat(indexActionExists(refLogic, refName)).isFalse();
        break;
      case REMOVE_REFERENCE_OTHER_POINTER:
        soft.assertThatThrownBy(() -> refLogic.deleteReference(refName, objIdFromString("0001")))
            .isInstanceOf(RefNotFoundException.class);
        soft.assertThat(persist.fetchReference(refName)).isNull();
        soft.assertThat(indexActionExists(refLogic, refName)).isFalse();
        break;
      default:
        throw new IllegalArgumentException();
    }

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REFS.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  private static boolean indexActionExists(ReferenceLogicImpl refLogic, String name) {
    StoreKey key = key(name);
    StoreIndexElement<CommitOp> el = refLogic.createRefsIndexSupplier().get().index().get(key);
    return el != null && el.content().action().exists();
  }

  /**
   * Verifies that commit-retries during {@link ReferenceLogicImpl#deleteReference(String, ObjId)}
   * work.
   */
  @Test
  public void deleteWithCommitRetry(
      @NessieStoreConfig(name = CONFIG_COMMIT_TIMEOUT_MILLIS, value = "300000") @NessiePersist
          Persist persist)
      throws Exception {
    Persist persistSpy = spy(persist);
    ReferenceLogicImpl refLogic = new ReferenceLogicImpl(persistSpy);

    ObjId initialPointer = randomObjId();
    String refName = "refs/foo/bar";
    ObjId extendedInfoObj = randomObjId();

    Reference reference = refLogic.createReference(refName, initialPointer, extendedInfoObj);
    soft.assertThat(reference)
        .extracting(
            Reference::name, Reference::pointer, Reference::deleted, Reference::extendedInfoObj)
        .containsExactly(refName, initialPointer, false, extendedInfoObj);

    Reference deleted = persist.markReferenceAsDeleted(reference);
    soft.assertThat(deleted.deleted()).isTrue();
    soft.assertThat(persist.fetchReference(refName)).isEqualTo(deleted);
    soft.assertThat(indexActionExists(refLogic, refName)).isTrue();

    singleCommitRetry(persistSpy);

    soft.assertThatThrownBy(() -> refLogic.deleteReference(refName, initialPointer))
        .isInstanceOf(RefNotFoundException.class);

    soft.assertThat(refLogic.getReferences(singletonList(refName))).hasSize(1).containsOnlyNulls();
    soft.assertThat(persist.fetchReference(refName)).isNull();
    soft.assertThat(indexActionExists(refLogic, refName)).isFalse();

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REFS.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }

  /**
   * Simulate the case when two users try to create the same reference with the same initial
   * pointer.
   */
  @Test
  public void concurrentCreateSameReferenceNameSameInitialPointer() throws Exception {
    ReferenceLogicImpl refLogic = new ReferenceLogicImpl(persist);

    ObjId initialPointer = randomObjId();
    ObjId extendedInfo1 = randomObjId();
    ObjId extendedInfo2 = randomObjId();
    String refName = "refs/foo/bar";

    // 1st user
    CommitReferenceResult commitCreate1 =
        refLogic.commitCreateReference(refName, initialPointer, extendedInfo1);
    soft.assertThat(commitCreate1.kind).isSameAs(ADDED_TO_INDEX);
    Reference ref1 = commitCreate1.reference;
    Reference expected =
        reference(refName, initialPointer, false, ref1.createdAtMicros(), extendedInfo1);
    soft.assertThat(ref1).isEqualTo(expected);
    // 2nd user
    CommitReferenceResult commitCreate2 =
        refLogic.commitCreateReference(refName, initialPointer, extendedInfo2);
    soft.assertThat(commitCreate2.kind).isSameAs(Kind.REF_ROW_MISSING);
    Reference ref2 = commitCreate2.reference;
    soft.assertThat(ref2).isEqualTo(expected);

    // 1st user
    soft.assertThat(persist.addReference(ref1)).isEqualTo(expected);
    // 2nd user
    soft.assertThatThrownBy(() -> persist.addReference(ref2))
        .isInstanceOf(RefAlreadyExistsException.class);

    soft.assertThat(
            newArrayList(
                commitLogic(persist)
                    .commitLog(commitLogQuery(persist.fetchReference(REF_REFS.name()).pointer()))))
        .allMatch(c -> c.commitType() == CommitType.INTERNAL);
  }
}
