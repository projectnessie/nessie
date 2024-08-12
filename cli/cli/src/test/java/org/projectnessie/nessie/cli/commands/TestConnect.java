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

import org.junit.jupiter.api.Test;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.nessie.cli.cli.NotConnectedException;
import org.projectnessie.nessie.cli.cmdspec.ImmutableConnectCommandSpec;

public class TestConnect extends BaseTestCommand {
  @Test
  public void connect() throws Exception {
    ImmutableConnectCommandSpec spec =
        ImmutableConnectCommandSpec.builder().uri(nessieBaseUri().toString()).build();

    try (NessieCliTester cli = unconnectedNessieCliTester()) {
      soft.assertThatThrownBy(cli::mandatoryNessieApi).isInstanceOf(NotConnectedException.class);

      soft.assertThatCode(() -> cli.execute(spec)).doesNotThrowAnyException();

      soft.assertThat(cli.capturedOutput())
          .containsExactly(
              format("Connecting to %s ...", nessieBaseUri()),
              format("No Iceberg REST endpoint at %s ...", nessieBaseUri().resolve("../iceberg/")),
              format(
                  "Successfully connected to Nessie REST at %s - Nessie API version 2, spec version 2.2.0",
                  nessieBaseUri()));

      soft.assertThatCode(cli::mandatoryNessieApi).doesNotThrowAnyException();
    }
  }

  @Test
  public void connectFail() throws Exception {
    String badUri = "http://127.0.0.1:42/api/v42";

    ImmutableConnectCommandSpec spec = ImmutableConnectCommandSpec.builder().uri(badUri).build();

    try (NessieCliTester cli = unconnectedNessieCliTester()) {
      soft.assertThatThrownBy(cli::mandatoryNessieApi).isInstanceOf(NotConnectedException.class);

      soft.assertThatThrownBy(() -> cli.execute(spec)).isInstanceOf(HttpClientException.class);

      soft.assertThat(cli.capturedOutput())
          .containsExactly(
              format("Connecting to %s ...", badUri),
              format("No Iceberg REST endpoint at %s ...", badUri));
    }
  }
}
