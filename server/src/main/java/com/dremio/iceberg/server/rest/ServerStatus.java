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

package com.dremio.iceberg.server.rest;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.model.AlleyConfiguration;
import com.dremio.iceberg.model.ImmutableAlleyConfiguration;
import com.dremio.iceberg.server.ServerConfiguration;
import com.dremio.iceberg.server.auth.Secured;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST endpoint to retrieve server settings.
 */
@Path("config")
@SecurityScheme(
    name = "iceberg-auth",
    type = SecuritySchemeType.HTTP,
    scheme = "bearer",
    bearerFormat = "JWT"
)
public class ServerStatus {

  private static final Logger logger = LoggerFactory.getLogger(ServerStatus.class);
  private final ServerConfiguration config;
  private final Backend backend;

  @Inject
  public ServerStatus(ServerConfiguration config, Backend backend) {
    this.config = config;
    this.backend = backend;
  }

  @GET
  @Metered
  @ExceptionMetered(name = "exception-readall")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-readall")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all configuration settings",
      tags = {"tables"},
      security = @SecurityRequirement(
        name = "iceberg-auth",
        scopes = "read:tables"),
      responses = {
        @ApiResponse(
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = AlleyConfiguration.class))),
        @ApiResponse(responseCode = "400", description = "Unknown Error")}
  )
  public AlleyConfiguration getConfig() {
    return ImmutableAlleyConfiguration.builder().defaultTag(this.config.getDefaultTag()).build();
  }


}
