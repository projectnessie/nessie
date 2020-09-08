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

package com.dremio.nessie.client.auth;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import com.dremio.nessie.auth.AuthResponse;
import com.dremio.nessie.client.NessieSimpleClient;
import com.dremio.nessie.client.RestUtils;

/**
 * Basic authentication module.
 */
public class BasicAuth implements Auth {

  private final String username;
  private final String password;
  private final NessieSimpleClient client;
  private String authHeader;
  private Instant expiryDate;

  /**
   * create new basic auth module.
   */
  public BasicAuth(String username, String password, NessieSimpleClient client) {
    this.username = username;
    this.password = password;
    this.client = client;
    login(username, password);
  }

  private void login(String username, String password) {
    Response response = client.login(username, password, "password");
    RestUtils.checkResponse(response);
    AuthResponse authToken = response.readEntity(AuthResponse.class);
    try {
      expiryDate = Instant.ofEpochMilli(Long.parseLong(authToken.expiryDate()));
    } catch (Exception e) {
      expiryDate = Instant.now().plus(5, ChronoUnit.MINUTES);
    }
    authHeader = response.getHeaderString(HttpHeaders.AUTHORIZATION);
  }

  /**
   * return key or renew if expired.
   */
  public String checkKey() {
    Instant now = Instant.now();
    if (now.isAfter(expiryDate)) {
      login(username, password);
    }
    return authHeader;
  }
}
