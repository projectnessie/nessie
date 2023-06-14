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
package org.projectnessie.restcatalog.server.auth;

import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;

/**
 * An {@link HttpAuthentication} implementation that simply adds the injected {@link
 * BearerTokenPropagator} bean as a request filter into the Nessie client, thus propagating the
 * bearer token from the incoming request to the Nessie client.
 */
public class BearerTokenPropagatingAuthentication implements HttpAuthentication {

  private final BearerTokenPropagator bearerTokenPropagator;

  public BearerTokenPropagatingAuthentication(BearerTokenPropagator bearerTokenPropagator) {
    this.bearerTokenPropagator = bearerTokenPropagator;
  }

  @Override
  public void applyToHttpClient(HttpClient.Builder client) {
    client.addRequestFilter(bearerTokenPropagator);
  }

  @Override
  public void close() {}
}
