/*
 * Copyright (C) 2020 Dremio
 *
 *             Licensed under the Apache License, Version 2.0 (the "License");
 *             you may not use this file except in compliance with the License.
 *             You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 *             Unless required by applicable law or agreed to in writing, software
 *             distributed under the License is distributed on an "AS IS" BASIS,
 *             WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *             See the License for the specific language governing permissions and
 *             limitations under the License.
 */

package com.dremio.iceberg.client;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;

import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;

public class AlleyClient implements AutoCloseable {

  private final String endpoint;
  private final Timer timer;
  private String authHeader;

  public AlleyClient(Configuration config) {
//    endpoint = "http://localhost:19120/api/v1"; // todo config driven & safety tests
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
  }

  private String login(String username, String password) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
      formData.add("username", username);
      formData.add("password", password);
      Response response = client.get(endpoint, "login", MediaType.APPLICATION_FORM_URLENCODED, null)
        .post(Entity.form(formData));
      if (response.getStatus() == 401) {
        throw new NotAuthorizedException(response.getEntity());
      }
      if (response.getStatus() != 200) {
        throw new UnknownError();//todo
      }
      return response.getHeaderString(HttpHeaders.AUTHORIZATION);
    }
  }

  public List<Table> getTables() {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, "tables/", MediaType.APPLICATION_JSON, authHeader).get();
      if (response.getStatus() != 200) {
        throw new RuntimeException(); //todo
      }
      Tables tables = response.readEntity(Tables.class);
      return tables.getTables();
    }
  }

  public Table getTable(String tableName) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, "tables/" + tableName, MediaType.APPLICATION_JSON, authHeader)
        .get();
//      if (response.getStatus() != 200) {
//        throw new RuntimeException(); //todo
//      }
      Table table = response.readEntity(Table.class);
      if (table != null) {
        String tag = response.getHeaders().getFirst(HttpHeaders.ETAG).toString();
        table.setEtag(tag);
      }
      return table;
    }
  }

  public void deleteTable(String tableName) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, "tables/" + tableName, MediaType.APPLICATION_JSON, authHeader)
        .delete();
      if (response.getStatus() != 200) {
        throw new RuntimeException(); //todo
      }
    }
  }

  public void updateTable(Table table) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Invocation.Builder request = client.get(endpoint, "tables/" + table.getTableName(),
        MediaType.APPLICATION_JSON, authHeader);
      if (table.getEtag() != null) {
        request.header(HttpHeaders.IF_MATCH, table.getEtag());
      }
      Response response = request.put(Entity.entity(table, MediaType.APPLICATION_JSON));
      if (response.getStatus() != 200) {
        throw new RuntimeException(); //todo
      }
    }
  }


  public Table createTable(Table table) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, "tables/", MediaType.APPLICATION_JSON, authHeader)
        .post(Entity.entity(table, MediaType.APPLICATION_JSON));
      if (response.getStatus() != 201) {
        throw new RuntimeException(); //todo
      }
      response = client.get(endpoint, "tables/" + table.getTableName(), MediaType.APPLICATION_JSON, authHeader)
        .get();
      Table newTable = response.readEntity(Table.class);
      newTable.setEtag(response.getHeaders().getFirst(HttpHeaders.ETAG).toString());
      return newTable;
    }
  }

  @Override
  public void close() throws IOException {
    timer.cancel();
  }

  private static class AutoCloseableClient implements AutoCloseable {
    private final Client client;

    private AutoCloseableClient() {
      this.client = ClientBuilder.newClient();
    }

    @Override
    public void close() {
      client.close();
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType, String authHeader) {
      WebTarget webTarget = client.target(endpoint);
      Invocation.Builder builder = webTarget.path(path).request(mediaType);
      if (authHeader != null) {
        builder.header(HttpHeaders.AUTHORIZATION, authHeader);
      }
      return builder;
    }
  }
}
