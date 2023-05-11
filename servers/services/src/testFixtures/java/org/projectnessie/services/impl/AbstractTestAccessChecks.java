/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.impl;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.FetchOption.ALL;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.model.MergeBehavior.NORMAL;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.v1.TreeApi;
import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessCheckException;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.versioned.DetachedRef;

public abstract class AbstractTestAccessChecks extends BaseTestServiceImpl {

  private static final String VIEW_MSG = "Must not view detached references";
  private static final String COMMITS_MSG = "Must not list from detached references";
  private static final String READ_MSG = "Must not read from detached references";
  private static final String ENTITIES_MSG = "Must not get entities from detached references";

  private static final Map<CheckType, String> CHECK_TYPE_MSG =
      ImmutableMap.of(
          CheckType.VIEW_REFERENCE, VIEW_MSG,
          CheckType.LIST_COMMIT_LOG, COMMITS_MSG,
          CheckType.READ_ENTITY_VALUE, ENTITIES_MSG,
          CheckType.READ_ENTRIES, READ_MSG);

  /**
   * Verify that response filtering for {@link TreeApi#getCommitLog(String, CommitLogParams)} and
   * {@link TreeApi#getEntries(String, EntriesParams)} does not return disallowed commit-log entries
   * / commit-operations.
   */
  @Test
  public void forbiddenContentKeys() throws Exception {
    Branch main = createBranch("forbiddenContentKeys");

    ContentKey keyForbidden1 = ContentKey.of("forbidden_1");
    ContentKey keyForbidden2 = ContentKey.of("forbidden_2");
    ContentKey keyAllowed1 = ContentKey.of("allowed_1");
    ContentKey keyAllowed2 = ContentKey.of("allowed_2");

    Branch commit =
        commit(
                main,
                CommitMeta.builder().message("no security context").build(),
                Put.of(keyForbidden1, IcebergTable.of(keyForbidden1.getName(), 42, 42, 42, 42)),
                Put.of(keyAllowed1, IcebergTable.of(keyAllowed1.getName(), 42, 42, 42, 42)),
                Put.of(keyForbidden2, IcebergTable.of(keyForbidden2.getName(), 42, 42, 42, 42)),
                Put.of(keyAllowed2, IcebergTable.of(keyAllowed2.getName(), 42, 42, 42, 42)))
            .getTargetBranch();

    ThrowingConsumer<Collection<ContentKey>> assertKeys =
        expectedKeys -> {
          assertThat(entries(commit))
              .extracting(EntriesResponse.Entry::getName)
              .containsExactlyInAnyOrderElementsOf(expectedKeys);
          assertThat(commitLog(commit.getName(), ALL, null))
              .hasSize(1)
              .element(0)
              .extracting(LogResponse.LogEntry::getOperations)
              .asInstanceOf(InstanceOfAssertFactories.list(Operation.class))
              .map(Operation::getKey)
              .containsExactlyInAnyOrderElementsOf(expectedKeys);
        };

    assertKeys.accept(Arrays.asList(keyAllowed1, keyAllowed2, keyForbidden1, keyForbidden2));

    setBatchAccessChecker(
        x ->
            new AbstractBatchAccessChecker() {
              @Override
              public Map<Check, String> check() {
                return getChecks().stream()
                    .filter(c -> c.type() == CheckType.READ_CONTENT_KEY)
                    .filter(
                        c ->
                            // forbid all content-keys starting with "forbidden"
                            requireNonNull(c.key()).getName().startsWith("forbidden"))
                    .collect(
                        Collectors.toMap(
                            Function.identity(),
                            c -> "Forbidden key " + requireNonNull(c.key()).getName()));
              }
            });

    assertKeys.accept(Arrays.asList(keyAllowed1, keyAllowed2));
  }

  @Test
  public void detachedRefAccessChecks() throws Exception {

    BatchAccessChecker accessChecker =
        new AbstractBatchAccessChecker() {
          @Override
          public Map<Check, String> check() {
            Map<Check, String> failed = new LinkedHashMap<>();
            getChecks()
                .forEach(
                    check -> {
                      String msg = CHECK_TYPE_MSG.get(check.type());
                      if (msg != null) {
                        if (check.ref() instanceof DetachedRef) {
                          failed.put(check, msg);
                        } else {
                          assertThat(check.ref().getName()).isNotEqualTo(DetachedRef.REF_NAME);
                        }
                      }
                    });
            return failed;
          }
        };

    setBatchAccessChecker(x -> accessChecker);

    Branch main = createBranch("committerAndAuthor");
    Branch merge = createBranch("committerAndAuthorMerge");
    Branch transplant = createBranch("committerAndAuthorTransplant");

    IcebergTable meta1 = IcebergTable.of("meep", 42, 42, 42, 42);
    ContentKey key = ContentKey.of("meep");
    Branch mainCommit =
        commit(
                main,
                CommitMeta.builder().message("no security context").build(),
                Put.of(key, meta1))
            .getTargetBranch();

    Branch detachedAsBranch = Branch.of(Detached.REF_NAME, mainCommit.getHash());
    Tag detachedAsTag = Tag.of(Detached.REF_NAME, mainCommit.getHash());
    Detached detached = Detached.of(mainCommit.getHash());

    for (Reference ref : Arrays.asList(detached, detachedAsBranch, detachedAsTag)) {
      soft.assertThatThrownBy(() -> commitLog(ref.getName(), MINIMAL, null, ref.getHash(), null))
          .describedAs("ref='%s', getCommitLog", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(COMMITS_MSG);
      soft.assertThatThrownBy(
              () ->
                  treeApi()
                      .mergeRefIntoBranch(
                          merge.getName(),
                          merge.getHash(),
                          ref.getName(),
                          ref.getHash(),
                          false,
                          null,
                          emptyList(),
                          NORMAL,
                          false,
                          false,
                          false))
          .describedAs("ref='%s', mergeRefIntoBranch", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(VIEW_MSG);
      soft.assertThatThrownBy(
              () ->
                  treeApi()
                      .transplantCommitsIntoBranch(
                          transplant.getName(),
                          transplant.getHash(),
                          null,
                          singletonList(ref.getHash()),
                          ref.getName(),
                          false,
                          emptyList(),
                          NORMAL,
                          false,
                          false,
                          false))
          .describedAs("ref='%s', transplantCommitsIntoBranch", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(VIEW_MSG);
      soft.assertThatThrownBy(() -> entries(ref))
          .describedAs("ref='%s', getEntries", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(READ_MSG);
      soft.assertThatThrownBy(
              () -> contentApi().getContent(key, ref.getName(), ref.getHash(), false))
          .describedAs("ref='%s', getContent", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(ENTITIES_MSG);
      soft.assertThatThrownBy(() -> diff(ref, main))
          .describedAs("ref='%s', getDiff1", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(VIEW_MSG);
      soft.assertThatThrownBy(() -> diff(main, ref))
          .describedAs("ref='%s', getDiff2", ref)
          .isInstanceOf(AccessCheckException.class)
          .hasMessageContaining(VIEW_MSG);
    }
  }
}
