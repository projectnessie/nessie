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
package org.projectnessie.tools.contentgenerator.cli;

import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_CLIENT_NAME;
import static org.projectnessie.client.config.NessieClientConfigSources.mapConfigSource;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import picocli.CommandLine;

@CommandLine.Command(
    name = "nessie-content-generator",
    mixinStandardHelpOptions = true,
    versionProvider = NessieVersionProvider.class,
    subcommands = {
      GenerateContent.class,
      ReadEntries.class,
      ReadCommits.class,
      ReadReferences.class,
      ReadContent.class,
      RefreshContent.class,
      DeleteContent.class,
      CopyContent.class,
      CreateMissingNamespaces.class,
      CommandLine.HelpCommand.class
    })
public abstract class ContentGenerator<API extends NessieApiV2> {

  @CommandLine.Option(
      names = {"-u", "--uri"},
      scope = CommandLine.ScopeType.INHERIT,
      description = "Nessie API endpoint URI, defaults to http://localhost:19120/api/v2.")
  private URI uri = URI.create("http://localhost:19120/api/v2");

  @CommandLine.Option(
      names = "--nessie-client",
      description = "Name of the Nessie client to use, defaults to HTTP suitable for REST.")
  private String nessieClientName;

  @CommandLine.Option(
      names = {"-o", "--nessie-option"},
      description = "Parameters to configure the NessieClientBuilder.",
      split = ",",
      arity = "0..*")
  private Map<String, String> nessieOptions = new HashMap<>();

  public API createNessieApiInstance() {
    @SuppressWarnings("unchecked")
    API api = (API) createNessieClientBuilder().build(NessieApiV2.class);
    return api;
  }

  public NessieClientBuilder createNessieClientBuilder() {
    Map<String, String> mainConfig = new HashMap<>(nessieOptions);
    if (nessieClientName != null) {
      mainConfig.put(CONF_NESSIE_CLIENT_NAME, nessieClientName);
    }
    NessieClientBuilder clientBuilder =
        createClientBuilderFromSystemSettings(mapConfigSource(mainConfig));
    if (uri != null) {
      clientBuilder.withUri(uri);
    }
    return clientBuilder;
  }
}
