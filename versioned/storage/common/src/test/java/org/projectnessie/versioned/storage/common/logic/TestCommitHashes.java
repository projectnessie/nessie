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
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.InstanceOfAssertFactories.BOOLEAN;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitLogic.ValueReplacement.NO_VALUE_REPLACEMENT;
import static org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution.ADD;
import static org.projectnessie.versioned.storage.common.logic.ConflictHandler.ConflictResolution.CONFLICT;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.commontests.AbstractCommitLogicTests.stdCommit;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestCommitHashes {

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  static Stream<List<Consumer<CreateCommit.Builder>>> hashesDifferChecks() {
    return Stream.of(
        asList(c -> c.message("other message"), c -> c.message("fairy tale")),
        asList(
            c -> c.addAdds(commitAdd(key("foo"), 42, EMPTY_OBJ_ID, null, null)),
            c -> c.addAdds(commitAdd(key("foo"), 42, EMPTY_OBJ_ID, null, randomUUID())),
            c -> c.addAdds(commitAdd(key("bar"), 42, EMPTY_OBJ_ID, null, null)),
            c -> c.addAdds(commitAdd(key("bar"), 42, objIdFromString("11223344"), null, null)),
            c -> c.addAdds(commitAdd(key("bar"), 43, EMPTY_OBJ_ID, null, null))),
        asList(
            c -> c.addRemoves(commitRemove(key("foo"), 42, EMPTY_OBJ_ID, null)),
            c -> c.addRemoves(commitRemove(key("foo"), 42, EMPTY_OBJ_ID, randomUUID())),
            c -> c.addRemoves(commitRemove(key("foo"), 43, EMPTY_OBJ_ID, null))),
        asList(
            c -> c.headers(newCommitHeaders().add("Foo", "bar").build()),
            c -> c.headers(newCommitHeaders().add("Foo", "bar").add("Foo", "foo").build()),
            c -> c.headers(newCommitHeaders().add("Foo", "bar").add("Bar", "foo").build())));
  }

  @ParameterizedTest
  @MethodSource("hashesDifferChecks")
  public void hashesDifferChecks(List<Consumer<CreateCommit.Builder>> modifiers) throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    CommitObj c1 =
        commitLogic.buildCommitObj(
            stdCommit().build(),
            c -> CONFLICT,
            (k, id) -> {},
            NO_VALUE_REPLACEMENT,
            NO_VALUE_REPLACEMENT);
    soft.assertThat(c1).isNotNull();

    Set<ObjId> ids = new HashSet<>();
    ids.add(c1.id());

    for (Consumer<CreateCommit.Builder> modifier : modifiers) {
      CreateCommit.Builder commit = stdCommit();

      modifier.accept(commit);

      CommitObj c2 =
          commitLogic.buildCommitObj(
              commit.build(), c -> ADD, (k, id) -> {}, NO_VALUE_REPLACEMENT, NO_VALUE_REPLACEMENT);
      soft.assertThat(c2)
          .describedAs("modified commit: %s", c2)
          .isNotNull()
          .extracting(CommitObj::id)
          .extracting(ids::add, BOOLEAN)
          .isTrue();
    }
  }

  static Stream<List<Consumer<CreateCommit.Builder>>> hashesDoNotDifferChecks() {
    return Stream.of(
        asList(
            c -> c.addAdds(commitAdd(key("foo"), 42, EMPTY_OBJ_ID, null, null)),
            c -> c.addAdds(commitAdd(key("foo"), 42, EMPTY_OBJ_ID, EMPTY_OBJ_ID, null)),
            c ->
                c.addAdds(
                    commitAdd(key("foo"), 42, EMPTY_OBJ_ID, objIdFromString("12341234"), null))),
        asList(
            c -> c.addSecondaryParents(objIdFromString("55667788")),
            c -> c.addSecondaryParents(objIdFromString("1111222233334444")),
            c ->
                c.addSecondaryParents(objIdFromString("55667788"))
                    .addSecondaryParents(objIdFromString("1111222233334444"))));
  }

  @ParameterizedTest
  @MethodSource("hashesDoNotDifferChecks")
  public void hashesDoNotDifferChecks(List<Consumer<CreateCommit.Builder>> modifiers)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);

    Iterator<Consumer<CreateCommit.Builder>> modifierIter = modifiers.iterator();
    CreateCommit.Builder commit = stdCommit();
    modifierIter.next().accept(commit);

    CommitObj c1 =
        commitLogic.buildCommitObj(
            commit.build(),
            c -> CONFLICT,
            (k, id) -> {},
            NO_VALUE_REPLACEMENT,
            NO_VALUE_REPLACEMENT);
    soft.assertThat(c1).isNotNull();

    while (modifierIter.hasNext()) {
      Consumer<CreateCommit.Builder> modifier = modifierIter.next();
      commit = stdCommit();

      modifier.accept(commit);

      CommitObj c2 =
          commitLogic.buildCommitObj(
              commit.build(), c -> ADD, (k, id) -> {}, NO_VALUE_REPLACEMENT, NO_VALUE_REPLACEMENT);
      soft.assertThat(c2)
          .describedAs("modified commit: %s", c2)
          .isNotNull()
          .extracting(CommitObj::id)
          .isEqualTo(c1.id());
    }
  }
}
