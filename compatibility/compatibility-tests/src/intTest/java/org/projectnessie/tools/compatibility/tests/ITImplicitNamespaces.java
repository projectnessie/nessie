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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.internal.NessieUpgradesExtension;

@ExtendWith(NessieUpgradesExtension.class)
@NessieServerProperty(name = "nessie.store.namespace-validation", value = "false")
@Tag("nessie-multi-env")
public class ITImplicitNamespaces {

  @NessieVersion Version version;
  @NessieAPI NessieApiV1 api;

  @Test
  void createTableWithImplicitNamespaceReference() throws Exception {
    Branch main = api.getDefaultBranch();
    Branch versionBranch = Branch.of("version-implicit-ns-" + version, main.getHash());
    api.createReference().sourceRefName(main.getName()).reference(versionBranch).create();

    // Check that keys with a non-existent namespace can be used in Put operations.
    // Note the server config annotations at the class level.
    Branch branchNew =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test implicit namespace in " + version))
            .operation(
                Put.of(
                    ContentKey.of("test-implicit-namespace-" + version, "table_name123"),
                    IcebergTable.of("metadata-location-" + version, 42L, 43, 44, 45)))
            .branch(versionBranch)
            .commit();
    assertThat(branchNew)
        .isNotEqualTo(versionBranch)
        .extracting(Branch::getName)
        .isEqualTo(versionBranch.getName());
  }
}
