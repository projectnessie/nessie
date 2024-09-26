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

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.NotConnectedException;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListContentsCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableUseReferenceCommandSpec;

@ExtendWith(SoftAssertionsExtension.class)
public class ITIcebergREST extends WithNessie {

  @InjectSoftAssertions protected SoftAssertions soft;

  @BeforeAll
  public static void start() throws Exception {
    setupObjectStoreAndNessie(emptyMap(), emptyMap());
  }

  @ParameterizedTest
  @MethodSource
  public void connectAndUse(
      String initialUri,
      String icebergConnectedUri,
      String initialReference,
      Reference expectedReference,
      String warehouse)
      throws Exception {
    ImmutableConnectCommandSpec.Builder specBuilder =
        ImmutableConnectCommandSpec.builder().uri(initialUri).initialReference(initialReference);
    if (warehouse != null) {
      specBuilder.putParameter("warehouse", warehouse);
    }
    ImmutableConnectCommandSpec spec = specBuilder.build();

    try (NessieCliTester cli = new NessieCliTester()) {
      soft.assertThatThrownBy(cli::mandatoryNessieApi).isInstanceOf(NotConnectedException.class);

      soft.assertThatCode(() -> cli.execute(spec)).doesNotThrowAnyException();

      soft.assertThat(cli.capturedOutput())
          .containsExactly(
              format("Connecting to %s ...", initialUri),
              format("Successfully connected to Iceberg REST at %s", icebergConnectedUri),
              format("Connecting to Nessie REST at %s/ ...", nessieApiUri),
              format(
                  "Successfully connected to Nessie REST at %s/ - Nessie API version 2, spec version 2.2.0",
                  nessieApiUri));

      soft.assertThatCode(cli::mandatoryNessieApi).doesNotThrowAnyException();

      soft.assertThat(cli.getCurrentReference()).isEqualTo(expectedReference);

      // "USE main" & check contents to verify that "USE" works for Nessie + Iceberg REST

      cli.execute(ImmutableUseReferenceCommandSpec.of(null, null, "BRANCH", "main", null));
      soft.assertThat(cli.getCurrentReference()).isEqualTo(defaultBranch);

      cli.execute(ImmutableListContentsCommandSpec.of(null, null, null, null, null, null, null));
      soft.assertThat(cli.capturedOutput())
          .containsExactly("      NAMESPACE tables", "  ICEBERG_TABLE " + tableOneKey);
      cli.execute(
          ImmutableShowContentCommandSpec.of(
              null, null, "TABLE", null, null, tableOneKey.toString()));
      soft.assertThat(cli.capturedOutput())
          .map(s -> s.endsWith(",") ? s.substring(0, s.length() - 1) : s)
          .contains(
              "        JSON: ",
              "Content type: ICEBERG_TABLE",
              " Content Key: " + tableOneKey,
              "          ID: " + tableOneId,
              //
              "Nessie metadata:",
              "  \"id\": \"" + tableOneId + "\"",
              "  \"metadataLocation\": \"" + tableOneMetadataLocation + "\"",
              //
              "Iceberg metadata:",
              "    \"nessie.catalog.content-id\": \"" + tableOneId + "\"",
              "    \"nessie.commit.id\": \"" + defaultBranch.getHash() + "\"",
              "    \"nessie.commit.ref\": \"" + defaultBranch.getName() + "\"");

      // "USE testBranch" & check contents to verify that "USE" works for Nessie + Iceberg REST

      cli.execute(
          ImmutableUseReferenceCommandSpec.of(null, null, "BRANCH", testBranch.getName(), null));
      soft.assertThat(cli.getCurrentReference()).isEqualTo(testBranch);

      cli.execute(ImmutableListContentsCommandSpec.of(null, null, null, null, null, null, null));
      soft.assertThat(cli.capturedOutput())
          .containsExactly("      NAMESPACE tables", "  ICEBERG_TABLE " + tableTwoKey);
      cli.execute(
          ImmutableShowContentCommandSpec.of(
              null, null, "TABLE", null, null, tableTwoKey.toString()));
      soft.assertThat(cli.capturedOutput())
          .map(s -> s.endsWith(",") ? s.substring(0, s.length() - 1) : s)
          .contains(
              "        JSON: ",
              "Content type: ICEBERG_TABLE",
              " Content Key: " + tableTwoKey,
              "          ID: " + tableTwoId,
              //
              "Nessie metadata:",
              "  \"id\": \"" + tableTwoId + "\"",
              "  \"metadataLocation\": \"" + tableTwoMetadataLocation + "\"",
              //
              "Iceberg metadata:",
              "    \"nessie.catalog.content-id\": \"" + tableTwoId + "\"",
              "    \"nessie.commit.id\": \"" + testBranch.getHash() + "\"",
              "    \"nessie.commit.ref\": \"" + testBranch.getName() + "\"");
    }
  }

  static Stream<Arguments> connectAndUse() {
    return Stream.of(
        // CONNECT TO with initial reference to "testBranch"
        arguments(icebergUri, icebergUri, testBranch.getName(), testBranch, null),
        // CONNECT TO with a "prefixed" Iceberg REST URI, but a different initial reference
        arguments(
            icebergUri + "/" + testBranch.getName() + "/",
            icebergUri + "/" + testBranch.getName() + "/",
            defaultBranch.getName(),
            defaultBranch,
            null),
        // CONNECT TO with a "prefixed" Iceberg REST URI
        arguments(
            icebergUri + "/" + testBranch.getName() + "/",
            icebergUri + "/" + testBranch.getName() + "/",
            null,
            testBranch,
            null),
        arguments(
            icebergUri + "/" + testBranch.getName(),
            icebergUri + "/" + testBranch.getName(),
            null,
            testBranch,
            null),
        // CONNECT TO with a Nessie API URI
        arguments(nessieApiUri, icebergUri + "/", null, defaultBranch, null),
        arguments(nessieApiUri + "/", icebergUri + "/", null, defaultBranch, null),
        // CONNECT TO with an Iceberg REST URI
        arguments(icebergUri, icebergUri, null, defaultBranch, null),
        arguments(icebergUri + "/", icebergUri + "/", null, defaultBranch, null),
        // S3 + GCS + ADLS
        arguments(icebergUri, icebergUri, null, defaultBranch, S3_WAREHOUSE),
        arguments(icebergUri, icebergUri, null, defaultBranch, GCS_WAREHOUSE),
        arguments(icebergUri, icebergUri, null, defaultBranch, ADLS_WAREHOUSE));
  }
}
