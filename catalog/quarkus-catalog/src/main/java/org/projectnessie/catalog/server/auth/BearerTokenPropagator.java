/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.server.auth;

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.RequestFilter;

@RequestScoped
public class BearerTokenPropagator implements RequestFilter {

  /**
   * The request headers.
   *
   * <p>Important: injection of @Context fields is made by JAX-RS, not by CDI. Most JAX-RS
   * implementations will only honor @Context fields when they are declared directly in a resource
   * class.
   *
   * <p>This injection point does not fall in that category; it is therefore honored only when the
   * JAX-RS implementation takes care of that. This is the case for RestEasy Reactive, because a
   * bridge between JAX-RS and CDI/Arc has been implemented; it will NOT be honored by other JAX-RS
   * impls, like RestEasy Classic. See <a
   * href="https://github.com/quarkusio/quarkus/issues/10496#issuecomment-887468525">this
   * comment</a> for context.
   */
  @Context HttpHeaders headers;

  @Override
  @ActivateRequestContext
  public void filter(RequestContext context) {
    String authorization = headers.getHeaderString("Authorization");
    if (authorization != null) {
      context.putHeader("Authorization", authorization);
    }
  }
}
