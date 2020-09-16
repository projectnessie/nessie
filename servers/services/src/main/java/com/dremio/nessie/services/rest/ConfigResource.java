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

package com.dremio.nessie.services.rest;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.metrics.annotation.Metered;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.api.ConfigApi;
import com.dremio.nessie.model.ImmutableNessieConfiguration;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.ServerConfig;


/**
 * REST endpoint to retrieve server settings.
 */
@Path("config")
@RequestScoped
public class ConfigResource implements ConfigApi {

  private static final Logger logger = LoggerFactory.getLogger(ConfigResource.class);
  private final ServerConfig config;

  @Inject
  public ConfigResource(ServerConfig config) {
    this.config = config;
  }

  @GET
  @Metered
  @Timed(name = "timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all configuration settings")

  @APIResponses({
        @APIResponse(
          description = "Configuration settings",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = NessieConfiguration.class))),
        @APIResponse(responseCode = "400", description = "Unknown Error")}
  )
  public NessieConfiguration getConfig() {
    return ImmutableNessieConfiguration.builder()
                                       .defaultBranch(this.config.getDefaultBranch()).build();
  }


}
