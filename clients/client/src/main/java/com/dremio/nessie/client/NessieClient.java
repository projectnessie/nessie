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

package com.dremio.nessie.client;

import com.dremio.nessie.auth.AuthResponse;
import com.dremio.nessie.client.RestUtils.ClientWithHelpers;
import com.dremio.nessie.client.auth.PublicKeyGenerator;
import com.dremio.nessie.jwt.JwtUtils;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Table;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

/**
 * Client side of Nessie. Performs HTTP requests to Server
 */
public class NessieClient implements AutoCloseable {

  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on('.');
  private static final Splitter UNSLASH = Splitter.on("/");

  private final String endpoint;
  private final String username;
  private final String password;
  private final ClientWithHelpers client;
  private String authHeader;
  private Date expiryDate;

  public NessieClient(String path, String username, String password) {
    endpoint = path;
    this.password = password;
    this.username = username;
    client = new ClientWithHelpers();
    login(username, password);
  }

  private void login(String username, String password) {
    MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
    formData.add("username", username);
    formData.add("password", password);
    Response response = client.get(endpoint, "login", MediaType.APPLICATION_FORM_URLENCODED, null)
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .post(Entity.form(formData));
    RestUtils.checkResponse(response);
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

  public NessieConfiguration getConfig() {
    Response response = client.get(endpoint, "config", MediaType.APPLICATION_JSON, checkKey())
                              .get();
    RestUtils.checkResponse(response);
    return response.readEntity(NessieConfiguration.class);
  }

  @Override
  public void close() {
    client.close();
  }

  public Table getTable(String branch, String name, String namespace) {
    String table = (namespace == null) ? name : (namespace + "." + name);
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch, table),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    try {
      RestUtils.checkResponse(response);
    } catch (NotFoundException e) {
      return null;
    }
    return response.readEntity(Table.class);
  }

  public Branch getBranch(String branchName) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branchName),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    try {
      RestUtils.checkResponse(response);
    } catch (NotFoundException e) {
      return null;
    }
    String pair = extractHeaders(response.getHeaders());
    Branch branch = response.readEntity(Branch.class);
    assert branch.getId().equals(pair.replaceAll("\"", ""));
    return branch;
  }

  public Branch createBranch(Branch branch) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getName()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .post(Entity.entity(branch, MediaType.APPLICATION_JSON_TYPE));
    RestUtils.checkResponse(response);
    return getBranch(branch.getName());
  }

  public void commit(Branch branch, Table... tables) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getName()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, new EntityTag(branch.getId()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .put(Entity.entity(tables, MediaType.APPLICATION_JSON_TYPE));
    RestUtils.checkResponse(response);
  }

  public Table[] getAllTables(String branch, String namespace) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch, "tables"),
                                   MediaType.APPLICATION_JSON,
                                   checkKey(),
                                   ImmutableMap.of("namespace",
                                                   namespace == null ? "all" : namespace))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    RestUtils.checkResponse(response);
    String[] tables = response.readEntity(String[].class);
    List<Table> branchTables = new ArrayList<>(tables.length);
    for (String table : tables) {
      branchTables.add(fromString(table));
    }
    return branchTables.toArray(new Table[0]);
  }

  private static String extractHeaders(MultivaluedMap<String, Object> headers) {
    return (String) headers.getFirst(HttpHeaders.ETAG);
  }

  private static Table fromString(String tableName) {
    String[] names = tableName.split("\\.");
    String namespace = null;
    if (names.length > 1) {
      namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
    }
    String name = names[names.length - 1];
    return ImmutableTable.builder()
                         .id(tableName)
                         .tableName(name)
                         .namespace(namespace)
                         .metadataLocation("")
                         .build();
  }

  public void updateBranch(com.dremio.nessie.model.Branch updatedTable,
                           String updateBranch,
                           boolean force) {
    Table[] branchTable = new Table[0];
    Response response = client.get(endpoint,
                                   SLASH.join("objects",
                                              updatedTable.getName(),
                                              "promote"),
                                   MediaType.APPLICATION_JSON,
                                   checkKey(),
                                   ImmutableMap.of("promote", updateBranch,
                                                   "force", Boolean.toString(force)))
                              .header(HttpHeaders.IF_MATCH, new EntityTag(updatedTable.getId()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .put(Entity.entity(branchTable, MediaType.APPLICATION_JSON_TYPE));
    RestUtils.checkResponse(response);
  }

  public void deleteBranch(Branch branch) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getName()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, new EntityTag(branch.getId()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .delete();
    RestUtils.checkResponse(response);
  }

  public void deleteTable(Branch branch, String updatedTable) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getName(), updatedTable),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, new EntityTag(branch.getId()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .delete();
    RestUtils.checkResponse(response);
  }
}
