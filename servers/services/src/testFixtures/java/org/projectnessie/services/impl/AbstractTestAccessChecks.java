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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.ALL;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.model.IdentifiedContentKey.IdentifiedElement.identifiedElement;
import static org.projectnessie.model.MergeBehavior.NORMAL;
import static org.projectnessie.services.authz.ApiContext.apiContext;
import static org.projectnessie.services.authz.Check.canCommitChangeAgainstReference;
import static org.projectnessie.services.authz.Check.canCreateEntity;
import static org.projectnessie.services.authz.Check.canDeleteEntity;
import static org.projectnessie.services.authz.Check.canReadEntityValue;
import static org.projectnessie.services.authz.Check.canUpdateEntity;
import static org.projectnessie.services.authz.Check.canViewReference;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.v1.TreeApi;
import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.IdentifiedContentKey.IdentifiedElement;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.UDF;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessCheckException;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.versioned.BranchName;
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

  protected Set<Check> recordAccessChecks() {
    Set<Check> checks = new HashSet<>();
    setBatchAccessChecker(
        c ->
            new AbstractBatchAccessChecker(apiContext("Nessie", 1)) {
              @Override
              public Map<Check, String> check() {
                checks.addAll(getChecks());
                return emptyMap();
              }
            });
    return checks;
  }

  @Test
  public void getContentForReadAndWrite() throws Exception {
    ContentKey keyNamespace = ContentKey.of("ns1");
    ContentKey keyTable = ContentKey.of("ns1", "key");
    ContentKey keyNonExisting = ContentKey.of("ns1", "notThere");
    Namespace namespace = Namespace.of(keyNamespace);
    IcebergTable table = IcebergTable.of("foo", 42, 42, 42, 42);

    BranchName branch = BranchName.of("getContentForReadAndWrite");
    Branch main = createBranch(branch.getName());

    CommitResponse commitResponse =
        commit(
            main,
            CommitMeta.builder().message("no security context").build(),
            Put.of(keyNamespace, namespace),
            Put.of(keyTable, table));

    Branch commit = commitResponse.getTargetBranch();
    namespace = commitResponse.contentWithAssignedId(keyNamespace, namespace);
    table = commitResponse.contentWithAssignedId(keyTable, table);

    IdentifiedElement elementNamespace =
        identifiedElement(keyNamespace.getName(), namespace.getId());
    IdentifiedElement elementTable = identifiedElement(keyTable.getName(), table.getId());
    IdentifiedElement elementNotThere = identifiedElement(keyNonExisting.getName(), null);

    IdentifiedContentKey identifiedKeyNamespace =
        IdentifiedContentKey.builder()
            .contentKey(keyNamespace)
            .type(namespace.getType())
            .addElements(elementNamespace)
            .build();
    IdentifiedContentKey identifiedKeyTable =
        IdentifiedContentKey.builder()
            .contentKey(keyTable)
            .type(table.getType())
            .addElements(elementNamespace, elementTable)
            .build();
    IdentifiedContentKey identifiedKeyNonExisting =
        IdentifiedContentKey.builder()
            .contentKey(keyNonExisting)
            .addElements(elementNamespace, elementNotThere)
            .build();

    // for read

    Set<Check> checks = recordAccessChecks();
    contents(commit, false, keyNamespace, keyTable, keyNonExisting);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canViewReference(branch),
            canReadEntityValue(branch, identifiedKeyNamespace),
            canReadEntityValue(branch, identifiedKeyTable));

    checks = recordAccessChecks();
    content(commit, false, keyNamespace);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canViewReference(branch), canReadEntityValue(branch, identifiedKeyNamespace));

    checks = recordAccessChecks();
    content(commit, false, keyTable);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canViewReference(branch), canReadEntityValue(branch, identifiedKeyTable));

    checks = recordAccessChecks();
    // Note: in practice the access-check throws before the content-not-found exception
    soft.assertThatThrownBy(() -> content(commit, false, keyNonExisting))
        .isInstanceOf(NessieContentNotFoundException.class);
    soft.assertThat(checks).containsExactlyInAnyOrder(canViewReference(branch));

    // for write

    checks = recordAccessChecks();
    contents(commit, true, keyNamespace, keyTable, keyNonExisting);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canCommitChangeAgainstReference(branch),
            canViewReference(branch),
            canUpdateEntity(branch, identifiedKeyNamespace),
            canUpdateEntity(branch, identifiedKeyTable),
            canCreateEntity(branch, identifiedKeyNonExisting),
            canReadEntityValue(branch, identifiedKeyNamespace),
            canReadEntityValue(branch, identifiedKeyTable),
            canReadEntityValue(branch, identifiedKeyNonExisting));

    checks = recordAccessChecks();
    content(commit, true, keyNamespace);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canCommitChangeAgainstReference(branch),
            canViewReference(branch),
            canUpdateEntity(branch, identifiedKeyNamespace),
            canReadEntityValue(branch, identifiedKeyNamespace));

    checks = recordAccessChecks();
    content(commit, true, keyTable);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canCommitChangeAgainstReference(branch),
            canViewReference(branch),
            canUpdateEntity(branch, identifiedKeyTable),
            canReadEntityValue(branch, identifiedKeyTable));

    checks = recordAccessChecks();
    // Note: in practice the access-check throws before the content-not-found exception
    soft.assertThatThrownBy(() -> content(commit, true, keyNonExisting))
        .isInstanceOf(NessieContentNotFoundException.class);
    soft.assertThat(checks)
        .containsExactlyInAnyOrder(
            canCommitChangeAgainstReference(branch),
            canViewReference(branch),
            canReadEntityValue(branch, identifiedKeyNonExisting),
            canCreateEntity(branch, identifiedKeyNonExisting));
  }

  @Test
  public void commitMergeTransplantAccessChecks() throws BaseNessieClientServerException {
    ContentKey keyUnrelated = ContentKey.of("unrelated");
    ContentKey keyNamespace1 = ContentKey.of("ns1");
    ContentKey keyNamespace2 = ContentKey.of("ns1", "ns2");
    ContentKey keyTable = ContentKey.of("ns1", "ns2", "key");
    Namespace namespace1 = Namespace.of(keyNamespace1);
    Namespace namespace2 = Namespace.of(keyNamespace2);
    IcebergTable table1 = IcebergTable.of("foo", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("bar", 42, 42, 42, 42);
    UDF unrelated = UDF.udf("udf-meta", "42", "666");

    Branch common = createBranch("common");
    CommitResponse commonResponse =
        commit(common, fromMessage("common"), Put.of(keyUnrelated, unrelated));
    common = commonResponse.getTargetBranch();
    unrelated = commonResponse.contentWithAssignedId(keyUnrelated, unrelated);

    Branch source = createBranch("source", common);
    Branch transplantTarget = createBranch("transplantTarget", common);

    // Commit

    Set<Check> checks = recordAccessChecks();
    CommitResponse response =
        commit(
            source,
            fromMessage("commit"),
            Put.of(keyNamespace1, namespace1),
            Put.of(keyNamespace2, namespace2),
            Put.of(keyTable, table1),
            Delete.of(keyUnrelated));
    source = response.getTargetBranch();
    Branch source1 = source;

    namespace1 = response.contentWithAssignedId(keyNamespace1, namespace1);
    namespace2 = response.contentWithAssignedId(keyNamespace2, namespace2);
    table1 = response.contentWithAssignedId(keyTable, table1);
    table2 = table2.withId(table1.getId());

    soft.assertThat(namespace1.getId()).isNotNull();
    soft.assertThat(namespace2.getId()).isNotNull();
    soft.assertThat(table1.getId()).isNotNull();

    IdentifiedElement elementNamespace1 =
        identifiedElement(keyNamespace1.getName(), namespace1.getId());
    IdentifiedElement elementNamespace2 =
        identifiedElement(keyNamespace2.getName(), namespace2.getId());
    IdentifiedElement elementTable = identifiedElement(keyTable.getName(), table1.getId());
    IdentifiedElement elementUnrelated =
        identifiedElement(keyUnrelated.getName(), unrelated.getId());

    IdentifiedContentKey identifiedKeyNamespace1 =
        IdentifiedContentKey.builder()
            .contentKey(keyNamespace1)
            .type(namespace1.getType())
            .addElements(elementNamespace1)
            .build();
    IdentifiedContentKey identifiedKeyNamespace2 =
        IdentifiedContentKey.builder()
            .contentKey(keyNamespace2)
            .type(namespace2.getType())
            .addElements(elementNamespace1, elementNamespace2)
            .build();
    IdentifiedContentKey identifiedKeyTable =
        IdentifiedContentKey.builder()
            .contentKey(keyTable)
            .type(table1.getType())
            .addElements(elementNamespace1, elementNamespace2, elementTable)
            .build();
    IdentifiedContentKey identifiedKeyUnrelated =
        IdentifiedContentKey.builder()
            .contentKey(keyUnrelated)
            .type(unrelated.getType())
            .addElements(elementUnrelated)
            .build();

    BranchName ref = BranchName.of(source.getName());
    soft.assertThat(checks)
        .contains(canCommitChangeAgainstReference(ref))
        .contains(canViewReference(ref))
        .contains(canCreateEntity(ref, identifiedKeyNamespace1))
        .contains(canCreateEntity(ref, identifiedKeyNamespace2))
        .contains(canCreateEntity(ref, identifiedKeyTable))
        .contains(canDeleteEntity(ref, identifiedKeyUnrelated));

    // Update entity on source branch

    checks = recordAccessChecks();
    response = commit(source, fromMessage("update"), Put.of(keyTable, table2));
    source = response.getTargetBranch();

    soft.assertThat(checks)
        .contains(canCommitChangeAgainstReference(ref))
        .contains(canViewReference(ref))
        .contains(canUpdateEntity(ref, identifiedKeyTable));

    // Merge

    checks = recordAccessChecks();
    treeApi()
        .mergeRefIntoBranch(
            common.getName(),
            common.getHash(),
            source.getName(),
            source.getHash(),
            null,
            emptyList(),
            NORMAL,
            false,
            false,
            false);

    ref = BranchName.of(common.getName());
    soft.assertThat(checks)
        .contains(canCommitChangeAgainstReference(ref))
        .contains(canViewReference(ref))
        .contains(canViewReference(BranchName.of(source.getName())))
        .contains(canCreateEntity(ref, identifiedKeyNamespace1))
        .contains(canCreateEntity(ref, identifiedKeyNamespace2))
        .contains(canCreateEntity(ref, identifiedKeyTable))
        .contains(canDeleteEntity(ref, identifiedKeyUnrelated));

    // Transplant

    checks = recordAccessChecks();
    treeApi()
        .transplantCommitsIntoBranch(
            transplantTarget.getName(),
            transplantTarget.getHash(),
            null,
            asList(source1.getHash(), source.getHash()),
            source1.getName(),
            emptyList(),
            NORMAL,
            false,
            false,
            false);

    ref = BranchName.of(transplantTarget.getName());
    soft.assertThat(checks)
        .contains(canCommitChangeAgainstReference(ref))
        .contains(canViewReference(ref))
        .contains(canViewReference(BranchName.of(source.getName())))
        .contains(canCreateEntity(ref, identifiedKeyNamespace1))
        .contains(canCreateEntity(ref, identifiedKeyNamespace2))
        .contains(canCreateEntity(ref, identifiedKeyTable))
        .contains(canUpdateEntity(ref, identifiedKeyTable))
        .contains(canDeleteEntity(ref, identifiedKeyUnrelated));
  }

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

    assertKeys.accept(asList(keyAllowed1, keyAllowed2, keyForbidden1, keyForbidden2));

    setBatchAccessChecker(
        x ->
            new AbstractBatchAccessChecker(apiContext("Nessie", 1)) {
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

    assertKeys.accept(asList(keyAllowed1, keyAllowed2));
  }

  @Test
  public void entriesAreFilteredBeforeAccessCheck() throws Exception {
    Branch main = createBranch("entriesAreFilteredBeforeAccessCheck");

    ContentKey someTableKey = ContentKey.of("sometablekey");

    Branch commit =
        commit(
                main,
                CommitMeta.builder().message("whatever").build(),
                Put.of(someTableKey, IcebergTable.of(someTableKey.getName(), 42, 42, 42, 42)))
            .getTargetBranch();

    setBatchAccessChecker(
        x ->
            new AbstractBatchAccessChecker(apiContext("Nessie", 1)) {
              @Override
              public Map<Check, String> check() {
                getChecks()
                    .forEach(
                        check -> {
                          if (Content.Type.ICEBERG_TABLE.equals(check.contentType())) {
                            throw new IllegalArgumentException("ALWAYS FAIL FOR TABLE ENTRY");
                          }
                        });
                return Collections.emptyMap();
              }
            });

    assertThat(entries(commit, null, "entry.contentType!='ICEBERG_TABLE'")).isEmpty();

    assertThatThrownBy(() -> entries(commit, null, "entry.contentType=='ICEBERG_TABLE'"))
        .hasMessageContaining("ALWAYS FAIL FOR TABLE ENTRY");
  }

  @Test
  public void detachedRefAccessChecks() throws Exception {

    BatchAccessChecker accessChecker =
        new AbstractBatchAccessChecker(apiContext("Nessie", 1)) {
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

    for (Reference ref : asList(detached, detachedAsBranch, detachedAsTag)) {
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
              () -> contentApi().getContent(key, ref.getName(), ref.getHash(), false, API_READ))
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

  @Test
  public void testCheckReadContentKeyOnDeletedEntity() throws Exception {
    Branch main = createBranch("testCheckReadContentKeyOnDeletedEntity");

    main =
        commit(
                main,
                CommitMeta.fromMessage("commit 1"),
                Put.of(ContentKey.of("t1"), IcebergTable.of("m1", 1, 2, 3, 4)))
            .getTargetBranch();

    main =
        commit(main, CommitMeta.fromMessage("commit 2"), Delete.of(ContentKey.of("t1")))
            .getTargetBranch();

    Set<Check> checks = recordAccessChecks();
    commitLog(main.getName(), ALL, null);

    assertThat(checks)
        .satisfiesOnlyOnce(
            check -> {
              assertThat(check.type()).isEqualTo(CheckType.READ_CONTENT_KEY);
              assertThat(check.key()).isEqualTo(ContentKey.of("t1"));
              assertThat(check.identifiedKey()).isNotNull();
              assertThat(check.contentId()).isNotNull();
            });
  }
}
