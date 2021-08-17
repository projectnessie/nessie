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
package org.projectnessie.client.auth;

import java.util.Objects;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.RequestFilter;

/** Filter to add "bearer" auth headers to all outgoing requests. */
public class BearerAuthFilter implements RequestFilter {

  private final String authHeaderValue;

  public BearerAuthFilter(String token) {
    Objects.requireNonNull(token, "Token must not be null for BEARER authentication");
    authHeaderValue = "Bearer " + token;
  }

  @Override
  public void filter(RequestContext context) {
    context.putHeader("Authorization", authHeaderValue);
  }
}
