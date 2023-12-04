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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.MINIMAL;

import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieReferenceAlreadyExistsException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operation.Unchanged;
import org.projectnessie.versioned.RepositoryInformation;

public abstract class AbstractTestMisc extends BaseTestServiceImpl {

  @Test
  public void testRepositoryInformation() {
    RepositoryInformation info = versionStore().getRepositoryInformation();
    soft.assertThat(info).isNotNull();
    soft.assertThat(info.getNoAncestorHash())
        .isNotNull()
        .isEqualTo(versionStore().noAncestorHash().asString());
    soft.assertThat(info.getRepositoryCreationTimestamp()).isNotNull();
    soft.assertThat(info.getOldestPossibleCommitTimestamp()).isNotNull();
  }

  @Test
  public void testSupportedApiVersions() {
    NessieConfiguration serverConfig = configApi().getConfig();
    NessieConfiguration builtInConfig = NessieConfiguration.getBuiltInConfig();
    assertThat(serverConfig)
        .extracting(
            NessieConfiguration::getMinSupportedApiVersion,
            NessieConfiguration::getMaxSupportedApiVersion,
            NessieConfiguration::getSpecVersion,
            NessieConfiguration::getActualApiVersion)
        .containsExactly(
            builtInConfig.getMinSupportedApiVersion(),
            builtInConfig.getMaxSupportedApiVersion(),
            builtInConfig.getSpecVersion(),
            2);
  }

  @Test
  public void checkSpecialCharacterRoundTrip() throws BaseNessieClientServerException {
    // ContentKey k = ContentKey.of("/%国","国.国");
    ContentKey key = ContentKey.of("a.b", "c.txt");
    Branch branch = ensureNamespacesForKeysExist(createBranch("specialchar"), key);
    IcebergTable table = IcebergTable.of("path1", 42, 42, 42, 42);
    commit(branch, fromMessage("commit 1"), Put.of(key, table));

    soft.assertThat(contents(branch.getName(), null, key))
        .containsKey(key)
        .hasEntrySatisfying(
            key,
            content ->
                assertThat(content)
                    .isEqualTo(IcebergTable.builder().from(table).id(content.getId()).build()));
  }

  @Test
  public void checkServerErrorPropagation() throws BaseNessieClientServerException {
    Branch branch = createBranch("bar");

    soft.assertThatThrownBy(() -> createBranch(branch.getName()))
        .isInstanceOf(NessieReferenceAlreadyExistsException.class)
        .hasMessageContaining("already exists");

    soft.assertThatThrownBy(
            () ->
                commit(
                    branch,
                    CommitMeta.builder()
                        .author("author")
                        .message("committed-by-test")
                        .committer("disallowed-client-side-committer")
                        .build(),
                    Unchanged.of(ContentKey.of("table"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot set the committer on the client side.");
  }

  @Test
  public void checkCelScriptFailureReporting() {
    String defaultBranch = config().getDefaultBranch();

    soft.assertThatThrownBy(() -> entries(defaultBranch, null, null, "invalid_script", false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");

    soft.assertThatThrownBy(() -> commitLog(defaultBranch, MINIMAL, "invalid_script"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("undeclared reference to 'invalid_script'");
  }
}
