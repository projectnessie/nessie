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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

@ExtendWith(OlderNessieServersExtension.class)
// Common merge parent is enforced only with the new data model, hence the `PERSIST` storage kind.
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")
@VersionCondition(minVersion = Version.CURRENT_STRING, maxVersion = Version.CURRENT_STRING)
@Tag("nessie-multi-env")
public class ITUnrelatedHistoryMerge {

  public static final String NO_ANCESTOR =
      "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";

  @NessieAPI NessieApiV2 api;

  @Test
  void mergeUnrelatedHistories() throws Exception {
    Branch main = api.getDefaultBranch();
    Branch branch1 = Branch.of("orphan1", NO_ANCESTOR);
    Branch branch2 = Branch.of("orphan2", NO_ANCESTOR);
    api.createReference().sourceRefName(main.getName()).reference(branch1).create();
    api.createReference().sourceRefName(main.getName()).reference(branch2).create();

    Branch branch1New =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test unrelated merge"))
            .operation(
                Put.of(
                    ContentKey.of("test1"), IcebergTable.of("metadata-location", 42L, 43, 44, 45)))
            .branch(branch1)
            .commit();
    Branch branch2New =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("test unrelated merge"))
            .operation(
                Put.of(
                    ContentKey.of("test2"), IcebergTable.of("metadata-location", 42L, 43, 44, 45)))
            .branch(branch2)
            .commit();

    assertThatThrownBy(
            () -> api.mergeRefIntoBranch().fromRef(branch1New).branch(branch2New).merge())
        .hasMessageContaining("No common ancestor");
  }
}
