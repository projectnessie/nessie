/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.commands;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.nessie.cli.cmdspec.ImmutableAlterNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableAssignReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableDropReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListReferencesCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableUseReferenceCommandSpec;

public class TestReferences extends BaseTestCommand {
  @Test
  public void createAssignDrop() throws Exception {
    try (NessieCliTester cli = nessieCliTester()) {

      Reference beginning = cli.getCurrentReference();

      cli.execute(ImmutableCreateNamespaceCommandSpec.of(null, null, "foo", null, Map.of()));
      Reference createFoo = cli.getCurrentReference();
      soft.assertThat(createFoo).isNotEqualTo(beginning);

      cli.execute(ImmutableCreateNamespaceCommandSpec.of(null, null, "foo.bar", null, Map.of()));
      Reference createFooBar = cli.getCurrentReference();
      soft.assertThat(createFooBar).isNotEqualTo(createFoo);

      cli.execute(
          ImmutableAlterNamespaceCommandSpec.of(
              null, null, "foo", null, Map.of("prop", "value"), Set.of()));
      Reference alterFoo = cli.getCurrentReference();
      soft.assertThat(alterFoo).isNotEqualTo(createFooBar);

      // create some references

      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "TAG", "tag1", null, alterFoo.getHash(), false));
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "TAG", "tag2", "main", createFooBar.getHash(), false));
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "TAG", "tag3from1", "tag1", null, false));
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "TAG", "tag4from1", "tag1", beginning.getHash(), false));

      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "BRANCH", "branch1", null, alterFoo.getHash(), false));
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "BRANCH", "branch2", "main", createFooBar.getHash(), false));
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "BRANCH", "branch3from1", "branch1", null, false));
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "BRANCH", "branch4from1", "branch1", beginning.getHash(), false));

      soft.assertThat(
              cli.execute(ImmutableListReferencesCommandSpec.of(null, null, null, null, null)))
          .containsExactly(
              " BRANCH branch1 @ " + alterFoo.getHash(),
              " BRANCH branch2 @ " + createFooBar.getHash(),
              " BRANCH branch3from1 @ " + alterFoo.getHash(),
              " BRANCH branch4from1 @ " + beginning.getHash(),
              " BRANCH main @ " + cli.getCurrentReference().getHash(),
              " TAG    tag1 @ " + alterFoo.getHash(),
              " TAG    tag2 @ " + createFooBar.getHash(),
              " TAG    tag3from1 @ " + alterFoo.getHash(),
              " TAG    tag4from1 @ " + beginning.getHash());

      // re-assign

      soft.assertThat(cli.execute(ImmutableShowReferenceCommandSpec.of(null, null, "tag2", null)))
          .containsExactly(
              "Reference type: TAG",
              "          Name: tag2",
              "      Tip/HEAD: " + createFooBar.getHash());
      cli.execute(
          ImmutableAssignReferenceCommandSpec.of(
              null, null, "TAG", "tag2", cli.getCurrentReference().getName(), null));
      soft.assertThat(cli.execute(ImmutableShowReferenceCommandSpec.of(null, null, "tag2", null)))
          .containsExactly(
              "Reference type: TAG",
              "          Name: tag2",
              "      Tip/HEAD: " + cli.getCurrentReference().getHash());

      soft.assertThat(
              cli.execute(ImmutableShowReferenceCommandSpec.of(null, null, "branch1", null)))
          .containsExactly(
              "Reference type: BRANCH",
              "          Name: branch1",
              "      Tip/HEAD: " + alterFoo.getHash());
      cli.execute(
          ImmutableAssignReferenceCommandSpec.of(
              null,
              null,
              "BRANCH",
              "branch1",
              cli.getCurrentReference().getName(),
              createFooBar.getHash()));
      soft.assertThat(
              cli.execute(ImmutableShowReferenceCommandSpec.of(null, null, "branch1", null)))
          .containsExactly(
              "Reference type: BRANCH",
              "          Name: branch1",
              "      Tip/HEAD: " + createFooBar.getHash());

      soft.assertThat(
              cli.execute(
                  ImmutableShowReferenceCommandSpec.of(
                      null, null, cli.getCurrentReference().getName(), null)))
          .containsExactly(
              "Reference type: BRANCH",
              "          Name: main",
              "      Tip/HEAD: " + cli.getCurrentReference().getHash());
      cli.execute(
          ImmutableAssignReferenceCommandSpec.of(
              null,
              null,
              "BRANCH",
              cli.getCurrentReference().getName(),
              createFoo.getName(),
              createFoo.getHash()));
      soft.assertThat(
              cli.execute(
                  ImmutableShowReferenceCommandSpec.of(
                      null, null, cli.getCurrentReference().getName(), null)))
          .containsExactly(
              "Reference type: BRANCH",
              "          Name: main",
              "      Tip/HEAD: " + createFoo.getHash());
      // Current reference has been re-assigned, verify that it's been changed
      soft.assertThat(cli.getCurrentReference()).isEqualTo(createFoo);

      cli.execute(ImmutableUseReferenceCommandSpec.of(null, null, "TAG", "tag1", null));
      soft.assertThat(cli.getCurrentReference()).isEqualTo(Tag.of("tag1", alterFoo.getHash()));

      cli.execute(ImmutableUseReferenceCommandSpec.of(null, null, "BRANCH", "branch2", null));
      soft.assertThat(cli.getCurrentReference())
          .isEqualTo(Branch.of("branch2", createFooBar.getHash()));

      // Must not drop current reference
      soft.assertThatIllegalArgumentException()
          .isThrownBy(
              () ->
                  cli.execute(
                      ImmutableDropReferenceCommandSpec.of(null, null, "BRANCH", "branch2", false)))
          .withMessage("Must not delete the current reference.");
      cli.execute(ImmutableUseReferenceCommandSpec.of(null, null, null, "main", null));
      cli.execute(ImmutableDropReferenceCommandSpec.of(null, null, "BRANCH", "branch2", false));
    }
  }
}
