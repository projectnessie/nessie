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

package com.dremio.nessie.server.rest;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.nessie.auth.AuthResponse;
import com.dremio.nessie.auth.ImmutableAuthResponse;
import com.dremio.nessie.auth.UserService;
import com.dremio.nessie.error.ImmutableNessieError;
import com.google.common.base.Throwables;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.nessie.auth.AuthResponse;
import com.dremio.nessie.auth.ImmutableAuthResponse;
import com.dremio.nessie.auth.UserService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

/**
 * REST endpoint to facilitate retrieving JWTs.
 */
@Path("login")
public class Login {

  private UserService userService;

  @Inject
  public Login(UserService userService) {
    this.userService = userService;
  }

  /**
   * POST operation for login. Follows the password type for Oauth2.
   */
  @POST
  @Metered
  @ExceptionMetered(name = "exception-login")
  @Timed(name = "timed-login")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Login endpoint",
      tags = {"login"},
      responses = {
        @ApiResponse(
          description = "Auth response object",
          content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = AuthResponse.class))),
        @ApiResponse(responseCode = "401", description = "Not authorized, wrong username/password")}
  )
  public Response login(@FormParam("username") String login,
                        @FormParam("password") String password,
                        @FormParam("grant_type") String grantType) {
    try {
      if (!grantType.equals("password")) {
        return unauthorized();
      }
      String token = userService.authorize(login, password);
      return Response.ok(ImmutableAuthResponse.builder().token(token).build())
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token).build();
    } catch (NotAuthorizedException e) {
      return unauthorized();
    } catch (Throwable t) {
      return exception(t);
    }
  }

  private Response exception(Throwable t) {
    return Response.status(Status.INTERNAL_SERVER_ERROR)
                   .entity(Entity.entity(
                     ImmutableNessieError.builder()
                                         .errorCode(Status.INTERNAL_SERVER_ERROR.getStatusCode())
                                         .statusMessage(
                                           Status.INTERNAL_SERVER_ERROR.getReasonPhrase())
                                         .errorMessage(t.getMessage())
                                         .stackTrace(Throwables.getStackTraceAsString(t))
                                         .build(), MediaType.APPLICATION_JSON))
                   .build();
  }

  private Response unauthorized() {
    return Response.status(Status.UNAUTHORIZED)
                   .entity(Entity.entity(
                     ImmutableNessieError.builder()
                                         .errorCode(Status.UNAUTHORIZED.getStatusCode())
                                         .statusMessage(Status.UNAUTHORIZED.getReasonPhrase())
                                         .errorMessage("not authorized")
                                         .build(),
                     MediaType.APPLICATION_JSON_TYPE))
                   .build();
  }
}
