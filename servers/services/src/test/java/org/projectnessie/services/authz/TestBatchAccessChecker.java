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
package org.projectnessie.services.authz;

import static com.google.common.collect.Maps.immutableEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.services.authz.ApiContext.apiContext;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.TagName;

public class TestBatchAccessChecker {

  @Test
  public void emptyDoesNotThrow() {
    BatchAccessChecker checker = newAccessChecker(checks -> Collections.emptyMap());
    checker
        .canViewReference(BranchName.of("foo"))
        .canListCommitLog(TagName.of("bar"))
        .canReadEntries(DetachedRef.INSTANCE);
    assertThatCode(checker::checkAndThrow).doesNotThrowAnyException();
  }

  @Test
  public void noChecksPass() {
    BatchAccessChecker checker = newAccessChecker(checks -> Collections.emptyMap());
    assertThatCode(checker::checkAndThrow).doesNotThrowAnyException();
  }

  @Test
  public void allChecksPass() {
    BatchAccessChecker checker = newAccessChecker(checks -> Collections.emptyMap());
    performChecks(checker, listWithAllCheckTypes());
    assertThatCode(checker::checkAndThrow).doesNotThrowAnyException();
  }

  @Test
  public void doubleChecks() {
    Check checkViewRef = Check.check(CheckType.VIEW_REFERENCE, BranchName.of("foo"));
    String msgViewRef = "no, you must not";

    Check checkListTagCommits = Check.check(CheckType.LIST_COMMIT_LOG, TagName.of("bar"));
    String msgListTagCommits = "don't look into bar";

    BatchAccessChecker checker =
        newAccessChecker(
            checks ->
                ImmutableMap.of(checkViewRef, msgViewRef, checkListTagCommits, msgListTagCommits));
    checker
        .canViewReference(BranchName.of("foo"))
        .canListCommitLog(TagName.of("bar"))
        .canReadEntries(DetachedRef.INSTANCE);
    assertThat(checker.check())
        .containsExactly(
            immutableEntry(checkViewRef, msgViewRef),
            immutableEntry(checkListTagCommits, msgListTagCommits));

    assertThatThrownBy(checker::checkAndThrow)
        .isInstanceOf(AccessCheckException.class)
        .hasMessage("%s, %s", msgViewRef, msgListTagCommits);
  }

  @Test
  public void allCheckTypes() {
    List<Check> allChecks = listWithAllCheckTypes();

    Map<Check, String> expectedErrors =
        allChecks.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    c -> "no no " + c.type(),
                    (a, b) -> a,
                    LinkedHashMap::new));

    BatchAccessChecker checker =
        newAccessChecker(
            checks -> {
              assertThat(checks).containsAnyElementsOf(allChecks);
              return expectedErrors;
            });

    performChecks(checker, allChecks);

    assertThat(checker.check()).containsAllEntriesOf(expectedErrors);

    String expectedMsg =
        allChecks.stream()
            .map(c -> String.format("no no %s", c.type().name()))
            .collect(Collectors.joining(", "));
    assertThatThrownBy(checker::checkAndThrow)
        .isInstanceOf(AccessCheckException.class)
        .hasMessage(expectedMsg);
  }

  private static void performChecks(BatchAccessChecker checker, List<Check> checks) {
    checks.forEach(c -> performCheck(checker, c));
  }

  private static void performCheck(BatchAccessChecker checker, Check c) {
    switch (c.type()) {
      case VIEW_REFERENCE:
        checker.canViewReference(c.ref());
        break;
      case CREATE_REFERENCE:
        checker.canCreateReference(c.ref());
        break;
      case DELETE_REFERENCE:
        checker.canDeleteReference(c.ref());
        break;
      case READ_ENTRIES:
        checker.canReadEntries(c.ref());
        break;
      case READ_CONTENT_KEY:
        checker.canReadContentKey(c.ref(), c.identifiedKey(), c.actions());
        break;
      case ASSIGN_REFERENCE_TO_HASH:
        checker.canAssignRefToHash(c.ref());
        break;
      case LIST_COMMIT_LOG:
        checker.canListCommitLog(c.ref());
        break;
      case COMMIT_CHANGE_AGAINST_REFERENCE:
        checker.canCommitChangeAgainstReference(c.ref());
        break;
      case READ_ENTITY_VALUE:
        checker.canReadEntityValue(c.ref(), c.identifiedKey(), c.actions());
        break;
      case CREATE_ENTITY:
        checker.canCreateEntity(c.ref(), c.identifiedKey(), c.actions());
        break;
      case UPDATE_ENTITY:
        checker.canUpdateEntity(c.ref(), c.identifiedKey(), c.actions());
        break;
      case DELETE_ENTITY:
        checker.canDeleteEntity(c.ref(), c.identifiedKey(), c.actions());
        break;
      case READ_REPOSITORY_CONFIG:
        checker.canReadRepositoryConfig(c.repositoryConfigType());
        break;
      case UPDATE_REPOSITORY_CONFIG:
        checker.canUpdateRepositoryConfig(c.repositoryConfigType());
        break;
      default:
        throw new IllegalArgumentException("Unsupported: " + c);
    }
  }

  private static List<Check> listWithAllCheckTypes() {
    return Arrays.stream(CheckType.values())
        .map(
            t -> {
              ImmutableCheck.Builder b = Check.builder(t);
              if (t.isRef()) {
                b.ref(BranchName.of("some-branch"));
              }
              if (t.isContent()) {
                b.key(ContentKey.of("hello", "my", "table")).contentId("cid-foo");
              }
              return b.build();
            })
        .collect(Collectors.toList());
  }

  static BatchAccessChecker newAccessChecker(
      Function<Collection<Check>, Map<Check, String>> check) {
    return new AbstractBatchAccessChecker(apiContext("Nessie", 1)) {
      @Override
      public Map<Check, String> check() {
        return check.apply(getChecks());
      }
    };
  }
}
