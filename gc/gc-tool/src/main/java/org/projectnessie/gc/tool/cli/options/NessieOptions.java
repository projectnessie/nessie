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
package org.projectnessie.gc.tool.cli.options;

import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.client.config.NessieClientConfigSources.mapConfigSource;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.gc.repository.NessieRepositoryConnector;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.gc.tool.cli.Closeables;
import picocli.CommandLine;

public class NessieOptions {

  @CommandLine.Option(
      names = "--nessie-client",
      description = "Name of the Nessie client to use, defaults to HTTP suitable for REST.")
  String nessieClientName;

  @CommandLine.Option(
      names = "--nessie-api",
      description =
          "Class name of the NessieClientBuilder implementation to use, defaults to HttpClientBuilder suitable for REST. "
              + "Using this parameter is not recommended. Prefer the --nessie-client parameter instead.")
  String nessieApi;

  @CommandLine.Option(
      names = {"-u", "--uri"},
      scope = CommandLine.ScopeType.INHERIT,
      description = "Nessie API endpoint URI, defaults to http://localhost:19120/api/v2.")
  URI nessieUri = URI.create("http://localhost:19120/api/v2");

  @CommandLine.Option(
      names = {"-o", "--nessie-option"},
      description = "Parameters to configure the NessieClientBuilder.",
      split = ",",
      arity = "0..*")
  Map<String, String> nessieOptions = new HashMap<>();

  public RepositoryConnector createRepositoryConnector(Closeables closeables) {
    return NessieRepositoryConnector.nessie(closeables.add(createNessieApi()));
  }

  @SuppressWarnings("deprecation")
  NessieApiV1 createNessieApi() {
    Map<String, String> baseConfig = new HashMap<>(nessieOptions);
    baseConfig.put(NessieConfigConstants.CONF_NESSIE_CLIENT_NAME, nessieClientName);
    baseConfig.put(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL, nessieApi);
    return createClientBuilderFromSystemSettings(mapConfigSource(baseConfig))
        .withUri(nessieUri)
        .build(NessieApiV2.class);
  }
}
