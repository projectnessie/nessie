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

import com.google.common.collect.ImmutableMap;
import java.security.AccessControlException;
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
import org.projectnessie.services.authz.ImmutableCheck.Builder;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.TagName;

public class BatchAccessCheckerTest {

  @Test
  public void emptyDoesNotThrow() {
    BatchAccessChecker checker = newAccessChecker(checks -> Collections.emptyMap());
    checker
        .canViewReference(BranchName.of("foo"))
        .canViewRefLog()
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
    Check checkRefLog = Check.builder(CheckType.VIEW_REFLOG).build();
    String msgRefLog = "no, you must not";

    Check checkListTagCommits =
        Check.builder(CheckType.LIST_COMMIT_LOG).ref(TagName.of("bar")).build();
    String msgListTagCommits = "don't look into bar";

    BatchAccessChecker checker =
        newAccessChecker(
            checks ->
                ImmutableMap.of(checkRefLog, msgRefLog, checkListTagCommits, msgListTagCommits));
    checker
        .canViewReference(BranchName.of("foo"))
        .canViewRefLog()
        .canListCommitLog(TagName.of("bar"))
        .canReadEntries(DetachedRef.INSTANCE);
    assertThat(checker.check())
        .containsExactly(
            immutableEntry(checkRefLog, msgRefLog),
            immutableEntry(checkListTagCommits, msgListTagCommits));

    assertThatThrownBy(checker::checkAndThrow)
        .isInstanceOf(AccessControlException.class)
        .hasMessage("%s, %s", msgRefLog, msgListTagCommits);
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
        .isInstanceOf(AccessControlException.class)
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
      case DELETE_DEFAULT_BRANCH:
        checker.canDeleteDefaultBranch();
        break;
      case READ_ENTRIES:
        checker.canReadEntries(c.ref());
        break;
      case READ_CONTENT_KEY:
        checker.canReadContentKey(c.ref(), c.key(), c.contentId());
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
        checker.canReadEntityValue(c.ref(), c.key(), c.contentId());
        break;
      case UPDATE_ENTITY:
        checker.canUpdateEntity(c.ref(), c.key(), c.contentId(), c.contentType());
        break;
      case DELETE_ENTITY:
        checker.canDeleteEntity(c.ref(), c.key(), c.contentId());
        break;
      case VIEW_REFLOG:
        checker.canViewRefLog();
        break;
      default:
        throw new IllegalArgumentException("Unsupported: " + c);
    }
  }

  private static List<Check> listWithAllCheckTypes() {
    return Arrays.stream(CheckType.values())
        .map(
            t -> {
              Builder b = Check.builder(t);
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
    return new AbstractBatchAccessChecker() {
      @Override
      public Map<Check, String> check() {
        return check.apply(getChecks());
      }
    };
  }
}
