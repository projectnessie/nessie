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

package com.dremio.nessie.server.auth;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.auth.User;
import com.dremio.nessie.auth.UserService;
import com.dremio.nessie.services.auth.Secured;

/**
 * Read cookie from request and validate it.
 */
@Secured
@Provider
@Priority(Priorities.AUTHENTICATION)
public class NessieAuthFilter implements ContainerRequestFilter {
  private static final Logger logger = LoggerFactory.getLogger(NessieAuthFilter.class);
  private final UserService userService;

  @Inject
  public NessieAuthFilter(UserService userService) {
    this.userService = userService;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    try {
      final String token = getTokenFromAuthHeaderOrQueryParameter(requestContext);
      User user = userService.validate(token);
      requestContext.setSecurityContext(new NessieSecurityContext(user, requestContext));
    } catch (NotAuthorizedException e) {
      logger.error("unauthorized", e);
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }
  }

  private static String getToken(final String input) {
    if (input != null) {
      return input.replace("Bearer", "").trim();
    }
    return null;
  }

  private static String getTokenFromAuthHeaderOrQueryParameter(ContainerRequestContext context)
      throws NotAuthorizedException {

    final String authHeader = getToken(context.getHeaderString(HttpHeaders.AUTHORIZATION));
    if (authHeader != null) {
      return authHeader;
    }

    final String token = getToken(context.getUriInfo()
                                         .getQueryParameters()
                                         .getFirst(HttpHeaders.AUTHORIZATION));
    if (token != null) {
      return token;
    }

    throw new NotAuthorizedException("Authorization header or access token must be provided");
  }
}
