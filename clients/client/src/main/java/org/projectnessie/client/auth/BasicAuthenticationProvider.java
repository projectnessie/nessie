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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_USERNAME;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestFilter;

/**
 * HTTP BASIC authentication provider.
 *
 * <p>Takes parameters {@link org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_USERNAME}
 * and {@link org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_PASSWORD}.
 */
public class BasicAuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "BASIC";

  public static HttpAuthentication create(String username, String password) {
    return new BasicAuthentication(username, password);
  }

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public HttpAuthentication build(Function<String, String> configSupplier) {
    String username = configSupplier.apply(CONF_NESSIE_USERNAME);
    if (username == null) {
      username = configSupplier.apply("nessie.username"); // legacy property name
    }

    String password = configSupplier.apply(CONF_NESSIE_PASSWORD);
    if (password == null) {
      password = configSupplier.apply("nessie.password"); // legacy property name
    }

    return new BasicAuthentication(username, password);
  }

  private static class BasicAuthentication implements HttpAuthentication {
    private final String authHeaderValue;

    @SuppressWarnings("QsPrivateBeanMembersInspection")
    private BasicAuthentication(String username, String password) {
      if (username == null || password == null) {
        throw new NullPointerException(
            "username and password parameters must be present for auth type " + AUTH_TYPE_VALUE);
      }

      String userPass = username + ':' + password;
      byte[] encoded = Base64.getEncoder().encode(userPass.getBytes(StandardCharsets.UTF_8));
      String encodedString = new String(encoded, StandardCharsets.UTF_8);
      this.authHeaderValue = "Basic " + encodedString;
    }

    @Override
    public void applyToHttpClient(HttpClient client) {
      client.register((RequestFilter) ctx -> ctx.putHeader("Authorization", authHeaderValue));
    }
  }
}
