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

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;

import com.dremio.iceberg.model.AlleyConfiguration;
import com.dremio.iceberg.model.BaseCollection;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;
import com.google.common.collect.ImmutableMap;

public class AlleyClient implements AutoCloseable {

  private final Timer timer;
  private final TableClient tableClient;
  private final TagClient tagClient;
  private final String endpoint;
  private String authHeader;

  public AlleyClient(Configuration config) {
    //todo die nicely when these aren't around
    String host = config.get("iceberg.alley.host");
    String port = config.get("iceberg.alley.port");
    boolean ssl = Boolean.parseBoolean(config.get("iceberg.alley.ssl"));
    String base = config.get("iceberg.alley.base");
    String username = config.get("iceberg.alley.username");
    String password = config.get("iceberg.alley.password");
    endpoint = (ssl ? "https" : "http") +
      "://" +
      host +
      ":" +
      port +
      "/" +
      base;
    authHeader = login(username, password);
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        authHeader = login(username, password);
      }
    };
    timer = new Timer("jwt_timer", true);
    timer.scheduleAtFixedRate(task, 1000 * 60 * 10, 1000 * 60 * 10);
    tableClient = new TableClient(base, endpoint, () -> authHeader);
    tagClient = new TagClient(base, endpoint, () -> authHeader);
  }

  private String login(String username, String password) {
    try (BaseClient.AutoCloseableClient client = new BaseClient.AutoCloseableClient()) {
      MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
      formData.add("username", username);
      formData.add("password", password);
      Response response = client.get(endpoint, "login", MediaType.APPLICATION_FORM_URLENCODED, null)
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.form(formData));
      BaseClient.checkResponse(response);
      return response.getHeaderString(HttpHeaders.AUTHORIZATION);
    }
  }

  @Override
  public void close() throws IOException {
    timer.cancel();
  }

  public AlleyConfiguration getConfig() {
    try (BaseClient.AutoCloseableClient client = new BaseClient.AutoCloseableClient()) {
      Response response = client.get(endpoint, "config", MediaType.APPLICATION_JSON, authHeader).get();
      BaseClient.checkResponse(response);
      return response.readEntity(AlleyConfiguration.class);
    }
  }

  public TableClient getTableClient() {
    return tableClient;
  }

  public TagClient getTagClient() {
    return tagClient;
  }
}
