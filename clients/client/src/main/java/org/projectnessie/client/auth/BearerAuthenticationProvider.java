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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TOKEN;

import java.util.Objects;
import java.util.function.Function;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestFilter;

public class BearerAuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "BEARER";

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public NessieAuthentication build(Function<String, String> configSupplier) {
    return create(configSupplier.apply(CONF_NESSIE_AUTH_TOKEN));
  }

  public static HttpAuthentication create(String token) {
    return new BearerAuthentication(token);
  }

  private static class BearerAuthentication implements HttpAuthentication {

    private final String authHeaderValue;

    private BearerAuthentication(String token) {
      Objects.requireNonNull(
          token, "Token must not be null for authentication type " + AUTH_TYPE_VALUE);
      authHeaderValue = "Bearer " + token;
    }

    @Override
    public void applyToHttpClient(HttpClient client) {
      client.register((RequestFilter) ctx -> ctx.putHeader("Authorization", authHeaderValue));
    }
  }
}
