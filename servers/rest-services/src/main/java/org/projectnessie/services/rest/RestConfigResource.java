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

import static org.projectnessie.services.rest.RestApiContext.NESSIE_V1;

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import org.projectnessie.api.v1.http.HttpConfigApi;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.spi.ConfigService;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint to retrieve server settings. */
@RequestScoped
@Path("api/v1/config")
public class RestConfigResource implements HttpConfigApi {

  private final ConfigService configService;

  // Mandated by CDI 2.0
  public RestConfigResource() {
    this(null, null, null, null);
  }

  @Inject
  public RestConfigResource(
      ServerConfig config, VersionStore store, Authorizer authorizer, AccessContext accessContext) {
    this.configService = new ConfigApiImpl(config, store, authorizer, accessContext, NESSIE_V1);
  }

  @Override
  @JsonView(Views.V1.class)
  public NessieConfiguration getConfig() {
    return configService.getConfig();
  }
}
