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
import com.dremio.nessie.client.BaseClient.ClientWithHelpers;
import com.dremio.nessie.client.auth.PublicKeyGenerator;
import com.dremio.nessie.client.branch.Branch;
import com.dremio.nessie.jwt.JwtUtils;
import com.dremio.nessie.model.BranchTable;
import com.dremio.nessie.model.HeadVersionPair;
import com.dremio.nessie.model.ImmutableBranchTable;
import com.dremio.nessie.model.NessieConfiguration;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.hadoop.conf.Configuration;

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

  public NessieClient(Configuration config) {
    //todo die nicely when these aren't around
    String path = config.get("nessie.url");
    String base = config.get("nessie.base");
    username = config.get("nessie.username");
    password = config.get("nessie.password");
    endpoint = path + "/" + base;
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

  public NessieConfiguration getConfig() {
    Response response = client.get(endpoint, "config", MediaType.APPLICATION_JSON, checkKey())
                              .get();
    BaseClient.checkResponse(response);
    return response.readEntity(NessieConfiguration.class);
  }

  @Override
  public void close() {
    client.close();
  }

  public BranchTable getTable(String branch, String name, String namespace) {
    String table = (namespace == null) ? name : (namespace + "." + name);
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch, table),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    try {
      BaseClient.checkResponse(response);
    } catch (NotFoundException e) {
      return null;
    }
    return response.readEntity(BranchTable.class);
  }

  public Branch getBranch(String branch) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    try {
      BaseClient.checkResponse(response);
    } catch (NotFoundException e) {
      return null;
    }
    HeadVersionPair pair = extractHeaders(response.getHeaders());
    return new NessieBranch(response.readEntity(com.dremio.nessie.model.Branch.class), this, pair);
  }

  public Branch createBranch(com.dremio.nessie.model.Branch branch) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getId()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .post(Entity.entity(branch, MediaType.APPLICATION_JSON_TYPE));
    BaseClient.checkResponse(response);
    return getBranch(branch.getId());
  }

  public void commit(Branch branch, BranchTable... tables) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.name()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, ((NessieBranch) branch).version())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .put(Entity.entity(tables, MediaType.APPLICATION_JSON_TYPE));
    BaseClient.checkResponse(response);
  }

  public BranchTable[] getAllTables(String branch, String namespace) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch),
                                   MediaType.APPLICATION_JSON,
                                   checkKey(),
                                   ImmutableMap.of("showtables", "true",
                                                   "namespace", namespace == null
                                                     ? "all" : namespace))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    BaseClient.checkResponse(response);
    String[] tables = response.readEntity(String[].class);
    List<BranchTable> branchTables = new ArrayList<>(tables.length);
    for (String table : tables) {
      branchTables.add(fromString(table));
    }
    return branchTables.toArray(new BranchTable[0]);
  }

  private static HeadVersionPair extractHeaders(MultivaluedMap<String, Object> headers) {
    Object headVersion = headers.getFirst(HttpHeaders.ETAG);
    Iterable<String> parts = UNSLASH.split((String) headVersion);
    Iterator<String> iter = parts.iterator();
    String head = iter.next().replace("\"", "");
    String version = iter.next().replace("\"", "");
    return new HeadVersionPair(head, Long.parseLong(version));
  }

  private static BranchTable fromString(String tableName) {
    String[] names = tableName.split("\\.");
    String namespace = null;
    if (names.length > 1) {
      namespace = DOT.join(Arrays.copyOf(names, names.length - 1));
    }
    String name = names[names.length - 1];
    return ImmutableBranchTable.builder()
                               .id(tableName)
                               .tableName(name)
                               .namespace(namespace)
                               .baseLocation("")
                               .metadataLocation("")
                               .build();
  }

  public void updateBranch(com.dremio.nessie.model.Branch updatedTable) {
    BranchTable[] branchTable = new BranchTable[0];
    Response response = client.get(endpoint,
                                   SLASH.join("objects", updatedTable.getId()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey(),
                                   ImmutableMap.of("promote", updatedTable.getName()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .put(Entity.entity(branchTable, MediaType.APPLICATION_JSON_TYPE));
    BaseClient.checkResponse(response);
  }

  public void deleteBranch(Branch branch, boolean purge) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.name()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, ((NessieBranch) branch).version())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .delete();
    BaseClient.checkResponse(response);
  }

  public void deleteTable(Branch branch, String updatedTable, boolean purge) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.name(), updatedTable),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, ((NessieBranch) branch).version())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .delete();
    BaseClient.checkResponse(response);
  }
}
