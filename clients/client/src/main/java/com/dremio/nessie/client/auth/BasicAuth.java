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

import com.dremio.nessie.auth.AuthResponse;

/**
 * Basic authentication module.
 */
public class BasicAuth implements Auth {

  private final String username;
  private final String password;
  private final AuthEndpointDefinition client;
  private String authHeader;
  private Instant expiryDate;

  /**
   * create new basic auth module.
   */
  public BasicAuth(String username, String password, AuthEndpointDefinition client) {
    this.username = username;
    this.password = password;
    this.client = client;
    login(username, password);
  }

  private void login(String username, String password) {
    AuthResponse authToken = client.login(username, password, "password");
    expiryDate = Instant.ofEpochMilli(Long.parseLong(authToken.expiryDate()));
    authHeader = String.format("Bearer %s", authToken.getToken());
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
