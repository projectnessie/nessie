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

package com.dremio.nessie.serverless;

import com.amazonaws.serverless.proxy.internal.servlet.AwsProxyHttpServletRequest;
import com.dremio.nessie.server.auth.Secured;
import java.security.Principal;
import javax.annotation.Priority;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;

/**
 * Read amazon lambda context from request and validate it.
 */
@Secured
@Provider
@Priority(Priorities.AUTHENTICATION)
public class IamAuthFilter implements ContainerRequestFilter {

  private static final String LAMBDA_PROPERTY = "com.amazonaws.serverless.jersey.servletRequest";

  private SecurityContext fromLambda(ContainerRequestContext requestContext) {
    AwsProxyHttpServletRequest request = (AwsProxyHttpServletRequest) requestContext.getProperty(
        LAMBDA_PROPERTY);
    return new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return request.getUserPrincipal();
      }

      @Override
      public boolean isUserInRole(String role) {
        return true; // User must be in role or it wouldn't have been invoked by the Gateway
      }

      @Override
      public boolean isSecure() {
        return request.isSecure();
      }

      @Override
      public String getAuthenticationScheme() {
        return request.getScheme();
      }
    };
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    try {
      SecurityContext securityContext = fromLambda(requestContext);
      requestContext.setSecurityContext(securityContext);
    } catch (NotAuthorizedException e) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }
  }

}
