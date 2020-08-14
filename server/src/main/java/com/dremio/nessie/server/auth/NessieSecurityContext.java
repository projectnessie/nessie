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

import java.security.Principal;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

import com.dremio.nessie.auth.User;

/**
 * Security context specific to Nessie.
 */
public class NessieSecurityContext implements SecurityContext {
  private final User user;
  private final ContainerRequestContext requestContext;

  public NessieSecurityContext(User user, ContainerRequestContext requestContext) {
    this.user = user;
    this.requestContext = requestContext;
  }

  @Override
  public Principal getUserPrincipal() {
    return user;
  }

  @Override
  public boolean isUserInRole(String role) {
    return user.isInRoles(role);
  }

  @Override
  public boolean isSecure() {
    return requestContext.getSecurityContext().isSecure();
  }

  @Override
  public String getAuthenticationScheme() {
    return requestContext.getSecurityContext().getAuthenticationScheme();
  }
}
