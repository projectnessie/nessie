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

package com.dremio.iceberg.client;

import com.dremio.iceberg.auth.AuthResponse;
import com.dremio.iceberg.client.BaseClient.ClientWithHelpers;
import com.dremio.iceberg.client.auth.PublicKeyGenerator;
import com.dremio.iceberg.jwt.JwtUtils;
import com.dremio.iceberg.model.AlleyConfiguration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import java.util.Date;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.hadoop.conf.Configuration;

/**
 * Client side of Iceberg Alley. Performs HTTP requests to Server
 */
public class AlleyClient implements AutoCloseable {

  private final TableClient tableClient;
  private final TagClient tagClient;
  private final String endpoint;
  private final String username;
  private final String password;
  private final ClientWithHelpers client;
  private String authHeader;
  private Date expiryDate;

  public AlleyClient(Configuration config) {
    //todo die nicely when these aren't around
    String path = config.get("iceberg.alley.url");
    String base = config.get("iceberg.alley.base");
    username = config.get("iceberg.alley.username");
    password = config.get("iceberg.alley.password");
    endpoint = path + "/" + base;
    client = new ClientWithHelpers();
    login(username, password);
    tableClient = new TableClient(client, base, endpoint, this::checkKey);
    tagClient = new TagClient(client, base, endpoint, this::checkKey);
  }

  private void login(String username, String password) {
    MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
    formData.add("username", username);
    formData.add("password", password);
    Response response = client.get(endpoint, "login", MediaType.APPLICATION_FORM_URLENCODED, null)
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .post(Entity.form(formData));
    BaseClient.checkResponse(response);
    AuthResponse authToken = response.readEntity(AuthResponse.class);
    try {
      Jws<Claims> claims = JwtUtils.checkToken(new PublicKeyGenerator(),
                                               authToken.getToken());
      expiryDate = claims.getBody().getExpiration();
    } catch (Exception e) {
      expiryDate = new Date(Long.MAX_VALUE);
    }
    authHeader = response.getHeaderString(HttpHeaders.AUTHORIZATION);
  }

  private String checkKey() {
    Date now = new Date();
    if (now.after(expiryDate)) {
      login(username, password);
    }
    return authHeader;
  }

  public AlleyConfiguration getConfig() {
    Response response = client.get(endpoint, "config", MediaType.APPLICATION_JSON, checkKey())
                              .get();
    BaseClient.checkResponse(response);
    return response.readEntity(AlleyConfiguration.class);
  }

  public TableClient getTableClient() {
    return tableClient;
  }

  public TagClient getTagClient() {
    return tagClient;
  }

  @Override
  public void close() {
    client.close();
  }
}
