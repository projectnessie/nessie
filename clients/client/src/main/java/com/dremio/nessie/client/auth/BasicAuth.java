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

import java.util.Date;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.dremio.nessie.auth.AuthResponse;
import com.dremio.nessie.client.RestUtils;
import com.dremio.nessie.client.RestUtils.ClientWithHelpers;
import com.dremio.nessie.jwt.JwtUtils;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwt;

/**
 * Basic authentication module.
 */
public class BasicAuth {
  private final String endpoint;
  private final String username;
  private final String password;
  private final ClientWithHelpers client;
  private String authHeader;
  private Date expiryDate;

  /**
   * create new basic auth module.
   */
  public BasicAuth(String endpoint,
                   String username,
                   String password,
                   ClientWithHelpers client) {
    this.endpoint = endpoint;
    this.username = username;
    this.password = password;
    this.client = client;
    login(username, password);
  }

  private void login(String username, String password) {
    MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
    formData.add("username", username);
    formData.add("password", password);
    formData.add("grant_type", "password");
    Response response = client.get(endpoint, "login", MediaType.APPLICATION_FORM_URLENCODED, null)
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .post(Entity.form(formData));
    RestUtils.checkResponse(response);
    AuthResponse authToken = response.readEntity(AuthResponse.class);
    try {
      Jwt<Header, Claims> claims = JwtUtils.checkToken(authToken.getToken());
      expiryDate = claims.getBody().getExpiration();
    } catch (Exception e) {
      expiryDate = new Date(Long.MAX_VALUE);
    }
    authHeader = response.getHeaderString(HttpHeaders.AUTHORIZATION);
  }

  /**
   * return key or renew if expired.
   */
  public String checkKey() {
    Date now = new Date();
    if (now.after(expiryDate)) {
      login(username, password);
    }
    return authHeader;
  }
}
