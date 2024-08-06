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

import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.cli.cmdspec.ImmutableAlterNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableDropContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListContentsCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowLogCommandSpec;

public class TestNamespaces extends BaseTestCommand {
  @Test
  public void createAlterDrop() throws Exception {
    try (NessieCliTester cli = nessieCliTester()) {

      ImmutableListContentsCommandSpec listContents =
          ImmutableListContentsCommandSpec.of(null, null, null, null, null, null, null);

      soft.assertThat(cli.execute(listContents)).containsExactly("");

      // Create two namespaces:
      //  foo, w/o properties
      //  foo.bar, w/ 1 property
      cli.execute(ImmutableCreateNamespaceCommandSpec.of(null, null, "foo", null, Map.of()));
      soft.assertThat(cli.execute(listContents)).containsExactly("      NAMESPACE foo");
      cli.execute(
          ImmutableCreateNamespaceCommandSpec.of(
              null, null, "foo.bar", null, Map.of("abc", "def")));
      soft.assertThat(cli.execute(listContents))
          .containsExactly("      NAMESPACE foo", "      NAMESPACE foo.bar");

      soft.assertThat(
              cli.execute(
                  ImmutableShowContentCommandSpec.of(null, null, "NAMESPACE", null, null, "foo")))
          .contains(
              "Content type: NAMESPACE",
              " Content Key: foo",
              // "          ID: ???",
              "        JSON: ",
              "{",
              "  \"type\": \"NAMESPACE\",",
              // "  \"id\": \"???\",",
              "  \"elements\": [",
              "    \"foo\"",
              "  ]",
              "}");
      soft.assertThat(
              cli.execute(
                  ImmutableShowContentCommandSpec.of(
                      null, null, "NAMESPACE", null, null, "foo.bar")))
          .contains(
              "Content type: NAMESPACE",
              " Content Key: foo.bar",
              // "          ID: ???",
              "        JSON: ",
              "{",
              "  \"type\": \"NAMESPACE\",",
              // "  \"id\": \"???\",",
              "  \"elements\": [",
              "    \"foo\",",
              "    \"bar\"",
              "  ],",
              "  \"properties\": {",
              "    \"abc\": \"def\"",
              "  }",
              "}");

      // update properties again
      cli.execute(
          ImmutableAlterNamespaceCommandSpec.of(
              null, null, "foo.bar", null, Map.of("abc", "foo", "bar", "baz"), List.of("abc")));
      soft.assertThat(
              cli.execute(
                  ImmutableShowContentCommandSpec.of(
                      null, null, "NAMESPACE", null, null, "foo.bar")))
          .contains(
              "Content type: NAMESPACE",
              " Content Key: foo.bar",
              // "          ID: ???",
              "        JSON: ",
              "{",
              "  \"type\": \"NAMESPACE\",",
              // "  \"id\": \"???\",",
              "  \"elements\": [",
              "    \"foo\",",
              "    \"bar\"",
              "  ],",
              "  \"properties\": {",
              "  }",
              "}");
      soft.assertThat(
              cli.mandatoryNessieApi()
                  .getContent()
                  .reference(cli.getCurrentReference())
                  .getSingle(ContentKey.of("foo", "bar")))
          .extracting(ContentResponse::getContent)
          .extracting(Namespace.class::cast)
          .extracting(
              Namespace::getProperties, InstanceOfAssertFactories.map(String.class, String.class))
          .hasSize(2)
          .containsEntry("abc", "foo")
          .containsEntry("bar", "baz");

      // update properties again
      cli.execute(
          ImmutableAlterNamespaceCommandSpec.of(
              null, null, "foo.bar", null, Map.of("cde", "foo", "bar", "baz"), List.of("abc")));
      List<String> fooBar =
          cli.execute(
              ImmutableShowContentCommandSpec.of(null, null, "NAMESPACE", null, null, "foo.bar"));
      soft.assertThat(fooBar)
          .contains(
              "Content type: NAMESPACE",
              " Content Key: foo.bar",
              // "          ID: ???",
              "        JSON: ",
              "{",
              "  \"type\": \"NAMESPACE\",",
              // "  \"id\": \"???\",",
              "  \"elements\": [",
              "    \"foo\",",
              "    \"bar\"",
              "  ],",
              "  \"properties\": {",
              "  }",
              "}");
      soft.assertThat(
              cli.mandatoryNessieApi()
                  .getContent()
                  .reference(cli.getCurrentReference())
                  .getSingle(ContentKey.of("foo", "bar")))
          .extracting(ContentResponse::getContent)
          .extracting(Namespace.class::cast)
          .extracting(
              Namespace::getProperties, InstanceOfAssertFactories.map(String.class, String.class))
          .hasSize(2)
          .containsEntry("cde", "foo")
          .containsEntry("bar", "baz");

      // Some ref/refHash combinations
      soft.assertThat(
              cli.execute(
                  ImmutableShowContentCommandSpec.of(
                      null,
                      null,
                      "NAMESPACE",
                      cli.getCurrentReference().getName(),
                      null,
                      "foo.bar")))
          .containsExactlyElementsOf(fooBar);
      soft.assertThat(
              cli.execute(
                  ImmutableShowContentCommandSpec.of(
                      null,
                      null,
                      "NAMESPACE",
                      cli.getCurrentReference().getName(),
                      cli.getCurrentReference().getHash(),
                      "foo.bar")))
          .containsExactlyElementsOf(fooBar);

      // Drop namespace
      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "TAG", "safething", null, null, false));

      cli.execute(ImmutableDropContentCommandSpec.of(null, null, "NAMESPACE", "foo.bar", null));
      cli.execute(
          ImmutableDropContentCommandSpec.of(
              null, null, "NAMESPACE", "foo", cli.getCurrentReference().getName()));

      soft.assertThat(cli.execute(listContents)).containsExactly("");

      soft.assertThat(
              cli.execute(
                  ImmutableListContentsCommandSpec.of(
                      null, null, "safething", null, null, null, null)))
          .containsExactly("      NAMESPACE foo", "      NAMESPACE foo.bar");

      // Verify commit log
      soft.assertThat(cli.execute(ImmutableShowLogCommandSpec.of(null, null, null, null, null)))
          .contains(
              "    Drop namespace foo",
              "    Drop namespace foo.bar",
              "    Alter namespace foo.bar",
              "    Create namespace foo.bar",
              "    Create namespace foo");
    }
  }
}
