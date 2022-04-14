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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;

@VersionCondition(maxVersion = Version.NOT_CURRENT_STRING)
public abstract class AbstractCompatibilityTests {

  @NessieAPI protected NessieApiV1 api;

  @Test
  void getDefaultBranch() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    assertThat(defaultBranch).extracting(Branch::getName).isEqualTo("main");

    ReferencesResponse allRefs = api.getAllReferences().get();
    assertThat(allRefs.getReferences()).contains(defaultBranch);
  }

  @Test
  void getConfig() {
    NessieConfiguration config = api.getConfig();
    assertThat(config).extracting(NessieConfiguration::getDefaultBranch).isEqualTo("main");
  }

  @Test
  void commit() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    Branch branch = Branch.of("commitToBranch", defaultBranch.getHash());
    Reference created =
        api.createReference().sourceRefName(defaultBranch.getName()).reference(branch).create();
    assertThat(created).isEqualTo(branch);

    ContentKey key = ContentKey.of("my", "tables", "table_name");
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
        .isNotEqualTo(branch)
        .extracting(Branch::getName)
        .isEqualTo(branch.getName());

    LogResponse commitLog = api.getCommitLog().refName(branch.getName()).get();
    assertThat(commitLog.getLogEntries())
        .hasSize(1)
        .map(LogEntry::getCommitMeta)
        .map(CommitMeta::getMessage)
        .containsExactly(commitMessage);

    assertThat(api.getContent().refName(branch.getName()).key(key).get())
        .containsEntry(key, content);
  }

  @Test
  @VersionCondition(minVersion = "0.23.1")
  public void namespace() throws NessieNotFoundException, NessieConflictException {
    Branch defaultBranch = api.getDefaultBranch();
    Branch branch = Branch.of("createNamespace", defaultBranch.getHash());
    Reference reference =
        api.createReference().sourceRefName(defaultBranch.getName()).reference(branch).create();

    Namespace namespace = Namespace.of("a", "b", "c");
    api.createNamespace().namespace(namespace).reference(reference).create();
    assertThat(api.getNamespace().namespace(namespace).reference(reference).get())
        .isEqualTo(namespace);
    assertThat(
            api.getMultipleNamespaces()
                .namespace(Namespace.EMPTY)
                .reference(reference)
                .get()
                .getNamespaces())
        .containsExactly(namespace);
    api.deleteNamespace().reference(reference).namespace(namespace).delete();
    assertThatThrownBy(() -> api.getNamespace().namespace(namespace).reference(reference).get())
        .isInstanceOf(NessieNamespaceNotFoundException.class);
  }

  @Test
  @VersionCondition(minVersion = "0.27.0")
  public void namespaceWithProperties() throws NessieNotFoundException, NessieConflictException {
    Branch defaultBranch = api.getDefaultBranch();
    Branch branch = Branch.of("namespaceWithProperties", defaultBranch.getHash());
    Reference reference =
        api.createReference().sourceRefName(defaultBranch.getName()).reference(branch).create();

    Map<String, String> properties = ImmutableMap.of("key1", "prop1", "key2", "prop2");
    Namespace namespace = Namespace.of(properties, "a", "b", "c");

    api.createNamespace().namespace(namespace).reference(reference).properties(properties).create();
    assertThat(api.getNamespace().namespace(namespace).reference(reference).get())
        .isEqualTo(namespace);

    api.updateProperties()
        .reference(branch)
        .namespace(namespace)
        .updateProperties(ImmutableMap.of("key3", "val3", "key1", "xyz"))
        .removeProperties(ImmutableSet.of("key2", "key5"))
        .update();
    namespace = api.getNamespace().reference(branch).namespace(namespace).get();
    assertThat(namespace.getProperties()).isEqualTo(ImmutableMap.of("key1", "xyz", "key3", "val3"));
  }
}
