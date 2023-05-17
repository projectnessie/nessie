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
package org.projectnessie.tools.compatibility.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;

@VersionCondition(maxVersion = Version.NOT_CURRENT_STRING)
public abstract class AbstractCompatibilityTests {

  @NessieAPI protected NessieApiV1 api;
  @NessieAPI protected NessieApiV2 apiV2;
  @NessieVersion Version version;

  abstract Version getClientVersion();

  Branch createMissingNamespaces(Branch branch, ContentKey namespaceKey)
      throws NessieNotFoundException {
    for (int i = 1; i <= namespaceKey.getElementCount(); i++) {
      try {
        api.createNamespace()
            .refName(branch.getName())
            .namespace(Namespace.of(namespaceKey.getElements().subList(0, i)))
            .create();
      } catch (NessieNamespaceAlreadyExistsException ignore) {
        // ignore
      }
    }
    return (Branch) api.getReference().refName(branch.getName()).get();
  }

  @SuppressWarnings("deprecation")
  Stream<Reference> allReferences() throws NessieNotFoundException {
    if (getClientVersion().isGreaterThanOrEqual(Version.CLIENT_RESULTS_NATIVE_STREAM)) {
      return api.getAllReferences().stream();
    } else {
      return StreamingUtil.getAllReferencesStream(api, Function.identity(), OptionalInt.empty());
    }
  }

  @SuppressWarnings("deprecation")
  Stream<LogEntry> commitLog(Function<GetCommitLogBuilder, GetCommitLogBuilder> configurer)
      throws NessieNotFoundException {
    if (getClientVersion().isGreaterThanOrEqual(Version.CLIENT_RESULTS_NATIVE_STREAM)) {
      return configurer.apply(api.getCommitLog()).stream();
    } else {
      return StreamingUtil.getCommitLogStream(api, configurer, OptionalInt.empty());
    }
  }

  @Test
  void getDefaultBranch() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    assertThat(defaultBranch).extracting(Branch::getName).isEqualTo("main");

    assertThat(allReferences()).contains(defaultBranch);
  }

  @Test
  @VersionCondition(minVersion = "0.47.0")
  void getDefaultBranchV2() throws Exception {
    Branch defaultBranch = apiV2.getDefaultBranch();
    assertThat(defaultBranch).extracting(Branch::getName).isEqualTo("main");
    assertThat(apiV2.getAllReferences().stream()).contains(defaultBranch);
  }

  @Test
  void getConfigV1() {
    NessieConfiguration config = api.getConfig();
    assertThat(config.getDefaultBranch()).isEqualTo("main");
    assertThat(config.getMinSupportedApiVersion()).isEqualTo(1);
    assertThat(config.getMaxSupportedApiVersion()).isBetween(1, 2);
    assertThat(config.getActualApiVersion()).isEqualTo(0);
    assertThat(config.getSpecVersion()).isNull();
  }

  @Test
  @VersionCondition(minVersion = "0.59.0")
  void getConfigV2() {
    NessieConfiguration config = apiV2.getConfig();
    assertThat(config.getDefaultBranch()).isEqualTo("main");
    assertThat(config.getMinSupportedApiVersion()).isEqualTo(1);
    assertThat(config.getMaxSupportedApiVersion()).isBetween(1, 2);
    assertThat(config.getActualApiVersion()).isBetween(0, 2);
    assertThat(config.getSpecVersion()).isIn(null, "2.0-beta.1", "2.0.0-beta.1");
  }

  @Test
  void commit() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    String branchName = "commitToBranch";
    Branch branch = Branch.of(branchName, defaultBranch.getHash());
    Reference created =
        api.createReference().sourceRefName(defaultBranch.getName()).reference(branch).create();
    assertThat(created).isEqualTo(branch);

    ContentKey key = ContentKey.of("my", "tables", "table_name");
    branch = createMissingNamespaces(branch, key.getParent());
    IcebergTable content = IcebergTable.of("metadata-location", 42L, 43, 44, 45, "content-id");
    String commitMessage = "hello world";
    Put operation = Put.of(key, content);
    Branch branchNew =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage(commitMessage))
            .operation(operation)
            .branch(branch)
            .commit();
    assertThat(branchNew)
        .isNotEqualTo(created)
        .isNotEqualTo(branch)
        .extracting(Branch::getName)
        .isEqualTo(branchName);

    Stream<LogEntry> commitLog = commitLog(b -> b.refName(branchName));
    assertThat(commitLog)
        .filteredOn(e -> !e.getCommitMeta().getMessage().startsWith("create namespace "))
        .hasSize(1)
        .map(LogEntry::getCommitMeta)
        .map(CommitMeta::getMessage)
        .containsExactly(commitMessage);

    assertThat(api.getContent().refName(branch.getName()).key(key).get())
        .containsEntry(key, content);
  }

  @Test
  public void namespace() throws NessieNotFoundException, NessieConflictException {
    Branch defaultBranch = api.getDefaultBranch();
    String branchName = "createNamespace";
    Branch branch = Branch.of(branchName, defaultBranch.getHash());
    api.createReference().sourceRefName(defaultBranch.getName()).reference(branch).create();

    Namespace namespaceNoContentId = Namespace.of("a", "b", "c");

    branch = createMissingNamespaces(branch, namespaceNoContentId.toContentKey().getParent());

    Namespace namespace =
        api.createNamespace().namespace(namespaceNoContentId).reference(branch).create();
    branch = (Branch) api.getReference().refName(branch.getName()).get();
    assertThat(api.getNamespace().namespace(namespaceNoContentId).reference(branch).get())
        .isEqualTo(namespace);
    assertThat(api.getNamespace().namespace(namespace).reference(branch).get())
        .isEqualTo(namespace);
    assertThat(
            api.getMultipleNamespaces()
                .namespace(Namespace.EMPTY)
                .reference(branch)
                .get()
                .getNamespaces())
        .extracting(Namespace::toContentKey)
        .containsExactlyInAnyOrder(
            namespace.toContentKey(),
            namespace.toContentKey().getParent(),
            namespace.toContentKey().getParent().getParent());
    api.deleteNamespace().reference(branch).namespace(namespace).delete();
    Reference finalRef = api.getReference().refName(branchName).get();
    assertThatThrownBy(() -> api.getNamespace().namespace(namespace).reference(finalRef).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class);
  }

  @Test
  @VersionCondition(minVersion = "0.27.0")
  public void namespaceWithProperties() throws NessieNotFoundException, NessieConflictException {
    Branch defaultBranch = api.getDefaultBranch();
    Branch branch = Branch.of("namespaceWithProperties", defaultBranch.getHash());
    api.createReference().sourceRefName(defaultBranch.getName()).reference(branch).create();

    Map<String, String> properties = ImmutableMap.of("key1", "prop1", "key2", "prop2");
    Namespace namespaceNoContentId = Namespace.of(properties, "a", "b", "c");

    branch = createMissingNamespaces(branch, namespaceNoContentId.toContentKey().getParent());

    Namespace namespace =
        api.createNamespace()
            .namespace(namespaceNoContentId)
            .reference(branch)
            .properties(properties)
            .create();
    branch = (Branch) api.getReference().refName(branch.getName()).get();
    assertThat(api.getNamespace().namespace(namespaceNoContentId).reference(branch).get())
        .isEqualTo(namespace);
    assertThat(api.getNamespace().namespace(namespace).reference(branch).get())
        .isEqualTo(namespace);

    api.updateProperties()
        .reference(branch)
        .namespace(namespace)
        .updateProperties(ImmutableMap.of("key3", "val3", "key1", "xyz"))
        .removeProperties(ImmutableSet.of("key2", "key5"))
        .update();
    namespace = api.getNamespace().refName(branch.getName()).namespace(namespace).get();
    assertThat(namespace.getProperties()).isEqualTo(ImmutableMap.of("key1", "xyz", "key3", "val3"));
  }

  @Test
  public void transplant() throws NessieNotFoundException, NessieConflictException {
    Branch defaultBranch = api.getDefaultBranch();
    Branch src = Branch.of("transplant-src", defaultBranch.getHash());
    Branch dest = Branch.of("transplant-dest", defaultBranch.getHash());

    api.createReference().sourceRefName(defaultBranch.getName()).reference(src).create();
    api.createReference().sourceRefName(defaultBranch.getName()).reference(dest).create();

    ContentKey key = ContentKey.of("my", "tables", "table_name");

    src = createMissingNamespaces(src, key.getParent());

    IcebergTable content =
        IcebergTable.of("metadata-location", 42L, 43, 44, 45, "content-id-transplant");
    String commitMessage = "hello world";
    Put operation = Put.of(key, content);
    Branch committed =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage(commitMessage))
            .operation(operation)
            .branch(src)
            .commit();

    dest = createMissingNamespaces(dest, key.getParent());

    MergeResponse response =
        api.transplantCommitsIntoBranch()
            .fromRefName(src.getName())
            .hashesToTransplant(Collections.singletonList(committed.getHash()))
            .branch(dest)
            .transplant();

    if (nessieWithMergeResponse()) {
      assertThat(response).isNotNull();
    } else {
      assertThat(response).isNull();
    }
  }

  @Test
  public void merge() throws NessieNotFoundException, NessieConflictException {
    Branch defaultBranch = api.getDefaultBranch();
    Branch src = Branch.of("merge-src", defaultBranch.getHash());
    Branch dest = Branch.of("merge-dest", defaultBranch.getHash());

    api.createReference().sourceRefName(defaultBranch.getName()).reference(src).create();
    api.createReference().sourceRefName(defaultBranch.getName()).reference(dest).create();

    ContentKey key = ContentKey.of("my", "tables", "table_name");

    src = createMissingNamespaces(src, key.getParent());

    IcebergTable content =
        IcebergTable.of("metadata-location", 42L, 43, 44, 45, "content-id-merge");
    String commitMessage = "hello world";
    Put operation = Put.of(key, content);
    Branch committed =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage(commitMessage))
            .operation(operation)
            .branch(src)
            .commit();

    MergeResponse response = api.mergeRefIntoBranch().fromRef(committed).branch(dest).merge();

    if (nessieWithMergeResponse()) {
      assertThat(response).isNotNull();
    } else {
      assertThat(response).isNull();
    }
  }

  boolean nessieWithMergeResponse() {
    return version.isGreaterThan(Version.HAS_MERGE_RESPONSE);
  }

  public void mergeBehavior() throws BaseNessieClientServerException {
    Branch defaultBranch = api.getDefaultBranch();
    Branch src = Branch.of("merge-behavior-src", defaultBranch.getHash());
    Branch dest = Branch.of("merge-behavior-dest", defaultBranch.getHash());

    api.createReference().sourceRefName(defaultBranch.getName()).reference(src).create();
    api.createReference().sourceRefName(defaultBranch.getName()).reference(dest).create();

    ContentKey key1 = ContentKey.of("table1");
    ContentKey key2 = ContentKey.of("table2");
    String commitMessage = "hello world";
    Branch committed =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage(commitMessage))
            .branch(src)
            .operation(Put.of(key1, IcebergTable.of("loc1", 1, 2, 3, 4)))
            .operation(Put.of(key2, IcebergTable.of("loc2", 1, 2, 3, 4)))
            .commit();

    MergeResponse response =
        api.mergeRefIntoBranch()
            .fromRef(committed)
            .branch(dest)
            .defaultMergeMode(MergeBehavior.DROP)
            .mergeMode(key2, MergeBehavior.NORMAL)
            .merge();

    if (nessieWithMergeResponse()) {
      assertThat(response).isNotNull();
      assertThat(response.getDetails())
          .extracting(
              MergeResponse.ContentKeyDetails::getKey,
              MergeResponse.ContentKeyDetails::getMergeBehavior,
              MergeResponse.ContentKeyDetails::getConflictType)
          .containsExactlyInAnyOrder(
              tuple(key1, MergeBehavior.DROP, MergeResponse.ContentKeyConflict.NONE),
              tuple(key2, MergeBehavior.NORMAL, MergeResponse.ContentKeyConflict.NONE));
    } else {
      assertThat(response).isNull();
    }

    Map<ContentKey, Content> contents =
        api.getContent().key(key1).key(key2).refName(dest.getName()).get();
    assertThat(contents.entrySet())
        .extracting(Map.Entry::getKey, e -> ((IcebergTable) e.getValue()).getMetadataLocation())
        .containsExactly(tuple(key2, "loc2"));
  }
}
