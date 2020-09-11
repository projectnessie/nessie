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

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.metrics.annotation.Metered;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.model.ImmutableNessieConfiguration;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.ServerConfig;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;

/**
 * REST endpoint to retrieve server settings.
 */
@Path("config")
@SecurityScheme(
    name = "nessie-auth",
    type = SecuritySchemeType.HTTP,
    scheme = "bearer",
    bearerFormat = "JWT"
)
@ApplicationScoped
public class ServerStatus {

  private static final Logger logger = LoggerFactory.getLogger(ServerStatus.class);
  private final ServerConfig config;

  @Inject
  public ServerStatus(ServerConfig config) {
    this.config = config;
  }

  @GET
  @Metered
  //@RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all configuration settings",
      tags = {"tables"},
      security = @SecurityRequirement(name = "nessie-auth"),
      responses = {
        @ApiResponse(
          description = "Configuration settings",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = NessieConfiguration.class))),
        @ApiResponse(responseCode = "400", description = "Unknown Error")}
  )
  public NessieConfiguration getConfig() {
    return ImmutableNessieConfiguration.builder()
                                       .defaultBranch(this.config.getDefaultBranch()).build();
  }


}
