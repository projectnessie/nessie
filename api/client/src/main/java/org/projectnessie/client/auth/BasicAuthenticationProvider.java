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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;
import java.util.function.Supplier;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;

/**
 * HTTP BASIC authentication provider.
 *
 * <p>Takes parameters {@link org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_USERNAME}
 * and {@link org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_PASSWORD}.
 */
@SuppressWarnings("deprecation") // Basic auth itself is deprecated
public class BasicAuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "BASIC";

  public static HttpAuthentication create(String username, String password) {
    return new BasicAuthentication(username, password);
  }

  public static HttpAuthentication create(String username, Supplier<String> passwordSupplier) {
    if (username == null || passwordSupplier == null) {
      throw new NullPointerException("username and password supplier must be non-null");
    }
    return new BasicAuthentication(username, passwordSupplier);
  }

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public HttpAuthentication build(Function<String, String> configSupplier) {
    String username = configSupplier.apply(NessieConfigConstants.CONF_NESSIE_USERNAME);
    if (username == null) {
      username = configSupplier.apply("nessie.username"); // legacy property name
    }

    String password = configSupplier.apply(NessieConfigConstants.CONF_NESSIE_PASSWORD);
    if (password == null) {
      password = configSupplier.apply("nessie.password"); // legacy property name
    }

    return new BasicAuthentication(username, password);
  }

  private static class BasicAuthentication implements HttpAuthentication {
    private final Supplier<String> headerSupplier;

    private static String basicHeader(String username, String password) {
      if (username == null || password == null) {
        throw new NullPointerException(
            "username and password parameters must be present for auth type " + AUTH_TYPE_VALUE);
      }

      String userPass = username + ':' + password;
      byte[] encoded = Base64.getEncoder().encode(userPass.getBytes(StandardCharsets.UTF_8));
      String encodedString = new String(encoded, StandardCharsets.UTF_8);
      return "Basic " + encodedString;
    }

    private BasicAuthentication(String username, String password) {
      String header = basicHeader(username, password);
      this.headerSupplier = () -> header;
    }

    private BasicAuthentication(String username, Supplier<String> passwordSupplier) {
      this.headerSupplier = () -> basicHeader(username, passwordSupplier.get());
    }

    @Override
    public void applyToHttpClient(HttpClient.Builder client) {
      client.addRequestFilter(this::applyToHttpRequest);
    }

    @Override
    public void applyToHttpRequest(RequestContext context) {
      context.putHeader("Authorization", headerSupplier.get());
    }
  }
}
