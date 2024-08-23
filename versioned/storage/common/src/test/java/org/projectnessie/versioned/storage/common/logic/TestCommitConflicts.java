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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.CONTENT_ID_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_DOES_NOT_EXIST;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_EXISTS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.PAYLOAD_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.VALUE_DIFFERS;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.commitConflict;
import static org.projectnessie.versioned.storage.common.logic.CommitLogic.ValueReplacement.NO_VALUE_REPLACEMENT;
import static org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution.CONFLICT;
import static org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution.DROP;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Unchanged.commitUnchanged;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.StoredObjResult.storedObjResult;
import static org.projectnessie.versioned.storage.commontests.AbstractCommitLogicTests.stdCommit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestCommitConflicts {

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  static void buildCommit(CommitLogic commitLogic, Consumer<CreateCommit.Builder> builderConsumer)
      throws CommitConflictException, ObjNotFoundException {
    CreateCommit.Builder builder = stdCommit();
    builderConsumer.accept(builder);
    commitLogic.buildCommitObj(
        builder.build(), c -> CONFLICT, (k, id) -> {}, NO_VALUE_REPLACEMENT, NO_VALUE_REPLACEMENT);
  }

  @Test
  public void conflictAddNewToExisting() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    StoreKey key = key("hello");
    ObjId addObjId = objIdFromString("0000");
    ObjId otherObjId = objIdFromString("0001");

    CommitObj tip =
        commitLogic.doCommit(
            stdCommit().addAdds(commitAdd(key, 0, addObjId, null, null)).build(), emptyList());
    assertThat(tip).isNotNull();

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(key, 0, addObjId, null, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: KEY_EXISTS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, KEY_EXISTS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(key, 0, otherObjId, null, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: KEY_EXISTS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, KEY_EXISTS));

    // verify key index
    verifyKeyIndex(key, addObjId, tip);

    // remove, try again

    CommitObj tip2 = removeAndTryAgain(commitLogic, key, addObjId, tip.id());

    commitLogic.doCommit(
        stdCommit()
            .parentCommitId(tip2.id())
            .addAdds(commitAdd(key, 0, addObjId, null, null))
            .build(),
        emptyList());

    // verify key index
    verifyKeyIndex(key, addObjId, tip);
  }

  private static StoreIndexElement<CommitOp> existingFromIndex(
      StoreIndex<CommitOp> index, StoreKey key) {
    StoreIndexElement<CommitOp> existing = index.get(key);
    return existing != null && existing.content().action().exists() ? existing : null;
  }

  @Test
  public void conflictWithWrongExpected() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    StoreKey key = key("hello");
    ObjId addObjId = objIdFromString("0000");
    ObjId wrongObjId = objIdFromString("0001");
    ObjId replaceObjId = objIdFromString("0002");

    CommitObj tip =
        commitLogic.doCommit(
            stdCommit().addAdds(commitAdd(key, 0, addObjId, null, null)).build(), emptyList());
    assertThat(tip).isNotNull();

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(key, 0, replaceObjId, wrongObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: VALUE_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, VALUE_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.message("hello")
                            .parentCommitId(tip.id())
                            .addRemoves(commitRemove(key, 0, wrongObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: VALUE_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, VALUE_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addUnchanged(commitUnchanged(key, 0, wrongObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: VALUE_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, VALUE_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(key, 0, replaceObjId, wrongObjId, randomUUID()))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: CONTENT_ID_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, CONTENT_ID_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addUnchanged(commitUnchanged(key, 0, replaceObjId, randomUUID()))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: CONTENT_ID_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, CONTENT_ID_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addRemoves(commitRemove(key, 0, wrongObjId, randomUUID()))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: CONTENT_ID_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, CONTENT_ID_DIFFERS));

    // verify key index
    verifyKeyIndex(key, addObjId, tip);
  }

  @Test
  public void conflictWithWrongPayload() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    StoreKey key = key("hello");
    ObjId addObjId = objIdFromString("0000");
    ObjId replaceObjId = objIdFromString("0002");

    CommitObj tip =
        commitLogic.doCommit(
            stdCommit().addAdds(commitAdd(key, 0, addObjId, null, null)).build(), emptyList());
    assertThat(tip).isNotNull();

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(key, 1, replaceObjId, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: PAYLOAD_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, PAYLOAD_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addRemoves(commitRemove(key, 1, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: PAYLOAD_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, PAYLOAD_DIFFERS));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addUnchanged(commitUnchanged(key, 1, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: PAYLOAD_DIFFERS:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, PAYLOAD_DIFFERS));

    // verify key index
    verifyKeyIndex(key, addObjId, tip);
  }

  @Test
  public void conflictNonExisting() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    StoreKey key = key("hello");
    StoreKey nonExistingKey = key("non-existing");
    ObjId addObjId = objIdFromString("0000");
    ObjId replaceObjId = objIdFromString("0002");

    CommitObj tip =
        commitLogic.doCommit(
            stdCommit().addAdds(commitAdd(key, 0, addObjId, null, null)).build(), emptyList());
    assertThat(tip).isNotNull();

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(nonExistingKey, 1, replaceObjId, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: KEY_DOES_NOT_EXIST:non-existing")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(nonExistingKey, KEY_DOES_NOT_EXIST));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addRemoves(commitRemove(nonExistingKey, 0, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: KEY_DOES_NOT_EXIST:non-existing")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(nonExistingKey, KEY_DOES_NOT_EXIST));

    // verify key index
    verifyKeyIndex(key, addObjId, tip);

    // remove added, try remove again
    CommitObj tip2 = removeAndTryAgain(commitLogic, key, addObjId, tip.id());

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip2.id())
                            .addRemoves(commitRemove(key, 0, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: KEY_DOES_NOT_EXIST:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, KEY_DOES_NOT_EXIST));

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip2.id())
                            .addUnchanged(commitUnchanged(key, 0, addObjId, null))))
        .isInstanceOf(CommitConflictException.class)
        .hasMessage("Commit conflict: KEY_DOES_NOT_EXIST:hello")
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::key, CommitConflict::conflictType)
        .containsExactly(tuple(key, KEY_DOES_NOT_EXIST));
  }

  StoreIndex<CommitOp> buildCompleteIndex(CommitObj c) {
    return indexesLogic(persist).buildCompleteIndex(c, Optional.empty());
  }

  private CommitObj removeAndTryAgain(
      CommitLogic commitLogic, StoreKey key, ObjId addObjId, ObjId tip)
      throws CommitConflictException, ObjNotFoundException {
    CommitObj tip2 =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit()
                    .parentCommitId(tip)
                    .addRemoves(commitRemove(key, 0, addObjId, null))
                    .build(),
                emptyList()));

    // verify key index
    soft.assertThat(tip2)
        .extracting(this::buildCompleteIndex)
        .satisfies(i -> assertThat(i.elementCount()).isEqualTo(1))
        .satisfies(i -> assertThat(existingFromIndex(i, key)).isNull())
        .extracting(i -> i.get(key))
        .extracting(StoreIndexElement::key, el -> el.content().value(), el -> el.content().action())
        .containsExactly(key, addObjId, REMOVE);
    return tip2;
  }

  private void verifyKeyIndex(StoreKey key, ObjId addObjId, CommitObj tip) {
    soft.assertThat(tip)
        .extracting(this::buildCompleteIndex)
        .satisfies(i -> assertThat(i.elementCount()).isEqualTo(1))
        .extracting(i -> existingFromIndex(i, key))
        .extracting(StoreIndexElement::key, el -> el.content().value(), el -> el.content().action())
        .containsExactly(key, addObjId, ADD);
  }

  @Test
  public void commitNonConflictCallback() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    Map<StoreKey, ObjId> callbacks = new LinkedHashMap<>();

    ObjId fooAddId = randomObjId();
    StoreKey fooKey = key("foo");

    ObjId barAddId = randomObjId();
    ObjId barUpdateId = randomObjId();
    StoreKey barKey = key("bar");

    // callback for an add

    CommitObj firstCommit =
        commitLogic.buildCommitObj(
            stdCommit()
                .addAdds(commitAdd(fooKey, 0, fooAddId, null, null))
                .addAdds(commitAdd(barKey, 0, barAddId, null, null))
                .build(),
            c -> CONFLICT,
            callbacks::put,
            NO_VALUE_REPLACEMENT,
            NO_VALUE_REPLACEMENT);

    soft.assertThat(callbacks)
        .hasSize(2)
        .containsEntry(fooKey, fooAddId)
        .containsEntry(barKey, barAddId);

    soft.assertThat(commitLogic.storeCommit(firstCommit, emptyList()))
        .isEqualTo(storedObjResult(firstCommit, true));
    ObjId firstCommitId = firstCommit.id();

    // callback for a remove + update

    callbacks.clear();

    CommitObj secondCommit =
        commitLogic.buildCommitObj(
            stdCommit()
                .parentCommitId(firstCommitId)
                .addRemoves(commitRemove(fooKey, 0, fooAddId, null))
                .addAdds(commitAdd(barKey, 0, barUpdateId, barAddId, null))
                .build(),
            c -> CONFLICT,
            callbacks::put,
            NO_VALUE_REPLACEMENT,
            NO_VALUE_REPLACEMENT);

    soft.assertThat(callbacks)
        .hasSize(2)
        .containsEntry(fooKey, null)
        .containsEntry(barKey, barUpdateId);

    soft.assertThat(commitLogic.storeCommit(secondCommit, emptyList()))
        .isEqualTo(storedObjResult(secondCommit, true));
    ObjId secondCommitId = secondCommit.id();
    CommitObj secondCommitLoaded = requireNonNull(commitLogic.fetchCommit(secondCommitId));
    commitLogic.updateCommit(secondCommitLoaded);
    CommitObj secondCommitUpdated = commitLogic.fetchCommit(secondCommitId);
    soft.assertThat(secondCommitUpdated).isEqualTo(secondCommitLoaded);

    // callback for a single remove

    callbacks.clear();

    commitLogic.buildCommitObj(
        stdCommit()
            .parentCommitId(secondCommitId)
            .addRemoves(commitRemove(barKey, 0, barUpdateId, null))
            .build(),
        c -> CONFLICT,
        callbacks::put,
        NO_VALUE_REPLACEMENT,
        NO_VALUE_REPLACEMENT);

    soft.assertThat(callbacks).hasSize(1).containsEntry(barKey, null);
  }

  @Test
  public void commitConflictCallbackKeyDoesNotExist() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    List<CommitConflict> conflicts = new ArrayList<>();
    Map<StoreKey, ObjId> callbacks = new LinkedHashMap<>();

    ObjId fooAddId = randomObjId();
    StoreKey fooKey = key("foo");

    ObjId barAddId = randomObjId();
    StoreKey barKey = key("bar");

    // ConflictResolution.CONFLICT

    CreateCommit.Add fooAddConflict = commitAdd(fooKey, 0, fooAddId, randomObjId(), null);

    soft.assertThatThrownBy(
            () ->
                commitLogic.buildCommitObj(
                    stdCommit()
                        .addAdds(fooAddConflict)
                        .addAdds(commitAdd(barKey, 0, barAddId, null, null))
                        .build(),
                    c -> {
                      conflicts.add(c);
                      return CONFLICT;
                    },
                    callbacks::put,
                    NO_VALUE_REPLACEMENT,
                    NO_VALUE_REPLACEMENT))
        .isInstanceOf(CommitConflictException.class)
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts, list(CommitConflict.class))
        .containsExactly(commitConflict(fooKey, KEY_DOES_NOT_EXIST, commitOp(ADD, 0, fooAddId)));

    soft.assertThat(callbacks).hasSize(1).containsEntry(barKey, barAddId);
    soft.assertThat(conflicts)
        .containsExactly(commitConflict(fooKey, KEY_DOES_NOT_EXIST, commitOp(ADD, 0, fooAddId)));

    // ConflictResolution.IGNORE

    callbacks.clear();
    conflicts.clear();

    CommitObj commit =
        commitLogic.buildCommitObj(
            stdCommit()
                .addAdds(fooAddConflict)
                .addAdds(commitAdd(barKey, 0, barAddId, null, null))
                .build(),
            c -> {
              conflicts.add(c);
              return ConflictHandler.ConflictResolution.ADD;
            },
            callbacks::put,
            NO_VALUE_REPLACEMENT,
            NO_VALUE_REPLACEMENT);

    soft.assertThat(callbacks).hasSize(1).containsEntry(barKey, barAddId);
    soft.assertThat(conflicts)
        .containsExactly(commitConflict(fooKey, KEY_DOES_NOT_EXIST, commitOp(ADD, 0, fooAddId)));
    soft.assertThat(indexesLogic.buildCompleteIndex(commit, Optional.empty()))
        .containsExactly(
            indexElement(barKey, commitOp(ADD, 0, barAddId)),
            indexElement(fooKey, commitOp(ADD, 0, fooAddConflict.value())));

    // ConflictResolution.DROP

    callbacks.clear();
    conflicts.clear();

    commit =
        commitLogic.buildCommitObj(
            stdCommit()
                .addAdds(fooAddConflict)
                .addAdds(commitAdd(barKey, 0, barAddId, null, null))
                .build(),
            c -> {
              conflicts.add(c);
              return DROP;
            },
            callbacks::put,
            NO_VALUE_REPLACEMENT,
            NO_VALUE_REPLACEMENT);

    soft.assertThat(callbacks).hasSize(1).containsEntry(barKey, barAddId);
    soft.assertThat(conflicts)
        .containsExactly(commitConflict(fooKey, KEY_DOES_NOT_EXIST, commitOp(ADD, 0, fooAddId)));
    soft.assertThat(indexesLogic.buildCompleteIndex(commit, Optional.empty()))
        .containsExactly(indexElement(barKey, commitOp(ADD, 0, barAddId)));
  }

  static Stream<Arguments> commitReaddContent() {
    UUID contentId1 = UUID.randomUUID();
    UUID contentId2 = UUID.randomUUID();
    return Stream.of(
        arguments(12, 12, null, null),
        arguments(12, 24, null, null),
        arguments(12, 12, contentId1, contentId1),
        arguments(12, 24, contentId1, contentId1),
        arguments(12, 12, contentId1, contentId2),
        arguments(12, 24, contentId1, contentId2));
  }

  @ParameterizedTest
  @MethodSource("commitReaddContent")
  public void commitReaddContent(int payload1, int payload2, UUID contentId1, UUID contentId2)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    StoreKey key = key("hello");
    ObjId addObjId = objIdFromString("0000");
    ObjId replaceObjId = objIdFromString("0002");

    CommitObj tip =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit().addAdds(commitAdd(key, payload1, addObjId, null, contentId1)).build(),
                emptyList()));

    StoreIndex<CommitOp> index =
        indexesLogic(persist).buildCompleteIndexOrEmpty(commitLogic.fetchCommit(tip.id()));
    soft.assertThat(index.get(key))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload1, addObjId, ADD, contentId1);

    soft.assertThatThrownBy(
            () ->
                buildCommit(
                    commitLogic,
                    b ->
                        b.parentCommitId(tip.id())
                            .addAdds(commitAdd(key, payload2, replaceObjId, null, contentId2))))
        .isInstanceOf(CommitConflictException.class);

    CommitObj tip2 =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit()
                    .parentCommitId(tip.id())
                    .addRemoves(commitRemove(key, payload1, addObjId, contentId1))
                    .addAdds(commitAdd(key, payload2, replaceObjId, null, contentId2))
                    .build(),
                emptyList()));

    index = indexesLogic(persist).buildCompleteIndexOrEmpty(commitLogic.fetchCommit(tip2.id()));
    soft.assertThat(index.get(key))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload2, replaceObjId, ADD, contentId2);
  }

  @Test
  public void commitRenameAndReAddMustFail() throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    ObjId addObjId = objIdFromString("0000");
    ObjId readdObjId = objIdFromString("1111");
    UUID contentId = UUID.randomUUID();

    int payload1 = 13;
    int payload2 = 14;
    StoreKey key1 = key("base");
    StoreKey key2 = key("rename-to");

    CommitObj tip =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit().addAdds(commitAdd(key1, payload1, addObjId, null, contentId)).build(),
                emptyList()));

    StoreIndex<CommitOp> index =
        indexesLogic(persist).buildCompleteIndexOrEmpty(commitLogic.fetchCommit(tip.id()));
    soft.assertThat(index.get(key1))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload1, addObjId, ADD, contentId);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                commitLogic.doCommit(
                    stdCommit()
                        .parentCommitId(tip.id())
                        .addRemoves(commitRemove(key1, payload1, addObjId, contentId))
                        // rename
                        .addAdds(commitAdd(key2, payload1, addObjId, null, contentId))
                        // re-add
                        .addAdds(commitAdd(key1, payload2, readdObjId, null, contentId))
                        .build(),
                    emptyList()))
        .withMessageStartingWith("Duplicate content ID");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                commitLogic.doCommit(
                    stdCommit()
                        .parentCommitId(tip.id())
                        .addRemoves(commitRemove(key1, payload1, addObjId, contentId))
                        // rename
                        .addAdds(commitAdd(key2, payload1, addObjId, addObjId, contentId))
                        // re-add
                        .addAdds(commitAdd(key1, payload2, readdObjId, addObjId, contentId))
                        .build(),
                    emptyList()))
        .withMessageStartingWith("Duplicate content ID");
  }

  static Stream<Arguments> commitRenameKey() {
    ObjId addObjId = objIdFromString("0000");
    UUID contentId = UUID.randomUUID();
    return Stream.of(
        // "rename" without expectedValue and without contentId just "goes through" - not strictly a
        // rename operation though
        arguments(addObjId, null, null),
        // rename should really, really provide the expectedValue AND contentId
        arguments(addObjId, addObjId, contentId));
  }

  @ParameterizedTest
  @MethodSource("commitRenameKey")
  public void commitRenameKey(ObjId addObjId, ObjId expectedValue, UUID contentId)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    int payload = 13;
    StoreKey key1 = key("rename-from");
    StoreKey key2 = key("rename-to");

    CommitObj tip =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit().addAdds(commitAdd(key1, payload, addObjId, null, contentId)).build(),
                emptyList()));

    StoreIndex<CommitOp> index =
        indexesLogic(persist).buildCompleteIndexOrEmpty(commitLogic.fetchCommit(tip.id()));
    soft.assertThat(index.get(key1))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload, addObjId, ADD, contentId);

    CommitObj tip2 =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit()
                    .parentCommitId(tip.id())
                    .addRemoves(commitRemove(key1, payload, addObjId, contentId))
                    .addAdds(commitAdd(key2, payload, addObjId, expectedValue, contentId))
                    .build(),
                emptyList()));

    index = indexesLogic(persist).buildCompleteIndexOrEmpty(commitLogic.fetchCommit(tip2.id()));
    soft.assertThat(index.get(key1))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload, addObjId, REMOVE, contentId);
    soft.assertThat(index.get(key2))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload, addObjId, ADD, contentId);
  }

  static Stream<Arguments> commitRenameKeyFailure() {
    ObjId addObjId = objIdFromString("0000");
    UUID contentId = UUID.randomUUID();
    return Stream.of(
        // "rename" with expectedValue but without a content ID is not recognized as a rename
        // operation, so the "add" action is treated like a normal add trying to _update_ an
        // existing key
        arguments(addObjId, addObjId, null, KEY_DOES_NOT_EXIST),
        // "rename" without expectedValue but with a content ID is treated as an incomplete rename
        // operation, because it does not provide the mandatory expectedValue
        arguments(addObjId, null, contentId, KEY_EXISTS));
  }

  @ParameterizedTest
  @MethodSource("commitRenameKeyFailure")
  public void commitRenameKeyFailure(
      ObjId addObjId, ObjId expectedValue, UUID contentId, ConflictType conflictType)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    int payload = 13;
    StoreKey key1 = key("rename-from");
    StoreKey key2 = key("rename-to");

    CommitObj tip =
        requireNonNull(
            commitLogic.doCommit(
                stdCommit().addAdds(commitAdd(key1, payload, addObjId, null, contentId)).build(),
                emptyList()));

    StoreIndex<CommitOp> index =
        indexesLogic(persist).buildCompleteIndexOrEmpty(commitLogic.fetchCommit(tip.id()));
    soft.assertThat(index.get(key1))
        .extracting(
            e -> e.content().payload(),
            e -> e.content().value(),
            e -> e.content().action(),
            e -> e.content().contentId())
        .containsExactly(payload, addObjId, ADD, contentId);

    soft.assertThatThrownBy(
            () ->
                commitLogic.doCommit(
                    stdCommit()
                        .parentCommitId(tip.id())
                        .addRemoves(commitRemove(key1, payload, addObjId, contentId))
                        .addAdds(commitAdd(key2, payload, addObjId, expectedValue, contentId))
                        .build(),
                    emptyList()))
        .isInstanceOf(CommitConflictException.class)
        .asInstanceOf(type(CommitConflictException.class))
        .extracting(CommitConflictException::conflicts)
        .asInstanceOf(list(CommitConflict.class))
        .extracting(CommitConflict::conflictType, CommitConflict::key)
        .containsExactly(tuple(conflictType, key2));
  }

  static Stream<Arguments> duplicateKeysInSingleCommit() {
    StoreKey key = key("hello");
    ObjId id = randomObjId();
    return Stream.of(
        arguments(
            "Add+Add",
            key,
            asList(
                commitAdd(key, 0, randomObjId(), null, null),
                commitAdd(key, 0, randomObjId(), null, null)),
            emptyList(),
            emptyList()),
        arguments(
            "Add+Unchanged",
            key,
            singletonList(commitAdd(key, 0, id, null, null)),
            singletonList(commitUnchanged(key, 0, id, null)),
            emptyList()),
        // Note: Remove+Add is a VALID scenario
        arguments(
            "Remove+Unchanged",
            key,
            emptyList(),
            singletonList(commitUnchanged(key, 0, id, null)),
            singletonList(commitRemove(key, 0, id, null))),
        arguments(
            "Remove+Remove",
            key,
            emptyList(),
            emptyList(),
            asList(commitRemove(key, 0, id, null), commitRemove(key, 0, id, null))));
  }

  @ParameterizedTest
  @MethodSource("duplicateKeysInSingleCommit")
  public void duplicateKeysInSingleCommit(
      String ignore,
      StoreKey key,
      List<CreateCommit.Add> adds,
      List<CreateCommit.Unchanged> unchanged,
      List<CreateCommit.Remove> removes) {
    CommitLogic commitLogic = commitLogic(persist);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              CreateCommit.Builder b = stdCommit().message("hello");
              adds.forEach(b::addAdds);
              removes.forEach(b::addRemoves);
              unchanged.forEach(b::addUnchanged);
              commitLogic.doCommit(b.build(), emptyList());
            })
        .withMessage("Duplicate key: " + key);
  }
}
