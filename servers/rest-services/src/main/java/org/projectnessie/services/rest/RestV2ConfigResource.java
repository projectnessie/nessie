/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.services.rest;

import static org.projectnessie.services.rest.RestApiContext.NESSIE_V2;

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.api.v2.http.HttpConfigApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.ImmutableRepositoryConfigResponse;
import org.projectnessie.model.ImmutableUpdateRepositoryConfigResponse;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.RepositoryConfigResponse;
import org.projectnessie.model.UpdateRepositoryConfigRequest;
import org.projectnessie.model.UpdateRepositoryConfigResponse;
import org.projectnessie.model.ser.Views;
import org.projectnessie.model.types.RepositoryConfigTypes;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint to retrieve server settings. */
@RequestScoped
@Path("api/v2/config")
public class RestV2ConfigResource implements HttpConfigApi {

  private final ConfigApiImpl config;

  // Mandated by CDI 2.0
  public RestV2ConfigResource() {
    this(null, null, null, null);
  }

  @Inject
  public RestV2ConfigResource(
      ServerConfig config, VersionStore store, Authorizer authorizer, AccessContext accessContext) {
    this.config = new ConfigApiImpl(config, store, authorizer, accessContext, NESSIE_V2);
  }

  @Override
  @JsonView(Views.V2.class)
  public NessieConfiguration getConfig() {
    return config.getConfig();
  }

  @Override
  public RepositoryConfigResponse getRepositoryConfig(List<String> repositoryConfigTypes) {
    Set<RepositoryConfig.Type> types =
        repositoryConfigTypes.stream()
            .map(RepositoryConfigTypes::forName)
            .collect(Collectors.toSet());
    return ImmutableRepositoryConfigResponse.builder()
        .addAllConfigs(config.getRepositoryConfig(types))
        .build();
  }

  @Override
  public UpdateRepositoryConfigResponse updateRepositoryConfig(
      UpdateRepositoryConfigRequest repositoryConfigUpdate) throws NessieConflictException {
    return ImmutableUpdateRepositoryConfigResponse.builder()
        .previous(config.updateRepositoryConfig(repositoryConfigUpdate.getConfig()))
        .build();
  }
}
