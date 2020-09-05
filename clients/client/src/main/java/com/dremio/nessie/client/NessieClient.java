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

import java.io.Closeable;
import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.dremio.nessie.client.RestUtils.ClientWithHelpers;
import com.dremio.nessie.client.auth.BasicAuth;
import com.dremio.nessie.client.rest.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.tracing.TracingUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

/**
 * Client side of Nessie. Performs HTTP requests to Server
 */
public class NessieClient implements Closeable {

  public enum AuthType {
    AWS,
    BASIC
  }

  static {
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    TracingUtil.initTracer("nessie-client");
  }

  private static final Joiner SLASH = Joiner.on("/");

  private final String endpoint;
  private final ClientWithHelpers client;
  private final BasicAuth auth;

  /**
   * create new nessie client. All REST api endpoints are mapped here.
   *
   * @param path URL for the nessie client (eg http://localhost:19120/api/v1)
   */
  public NessieClient(AuthType authType, String path, String username, String password) {
    endpoint = path;
    client = new ClientWithHelpers(authType == AuthType.AWS);
    auth = (authType == AuthType.AWS) ? null : new BasicAuth(endpoint, username, password, client);
  }

  private String checkKey() {
    return auth == null ? null : auth.checkKey();
  }

  /**
   * Fetch configuration from the server.
   */
  public NessieConfiguration getConfig() {
    Response response = client.get(endpoint, "config", MediaType.APPLICATION_JSON, checkKey())
                              .get();
    RestUtils.checkResponse(response);
    return response.readEntity(NessieConfiguration.class);
  }

  /**
   * Fetch all known branches from the server.
   */
  public Iterable<Branch> getBranches() {
    Response response = client.get(endpoint, "objects", MediaType.APPLICATION_JSON, checkKey())
                              .get();
    RestUtils.checkResponse(response);
    return response.readEntity(new GenericType<List<Branch>>() {
    });
  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * Get a single table specific to a given branch.
   */
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
    } catch (NessieNotFoundException e) {
      return null;
    }
    return response.readEntity(Table.class);
  }

  /**
   * Get a branch for a given name.
   */
  public Branch getBranch(String branchName) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branchName),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    try {
      RestUtils.checkResponse(response);
    } catch (NessieNotFoundException e) {
      return null;
    }
    String pair = extractHeaders(response.getHeaders());
    ReferenceWithType<Branch> branch = response.readEntity(new GenericType<ReferenceWithType<Branch>>(){});
    assert branch.getReference().getId().equals(pair.replaceAll("\"", ""));
    return branch.getReference();
  }

  /**
   * Create a new branch. Branch name is the branch name and id is the branch to copy from.
   */
  public Branch createBranch(Branch branch) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getName()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .post(Entity.entity(ReferenceWithType.of(branch), MediaType.APPLICATION_JSON_TYPE));
    RestUtils.checkResponse(response);
    return getBranch(branch.getName());
  }

  /**
   * Commit a set of tables on a given branch.
   *
   * <p>
   * These could be updates, creates or deletes given the state of the backend and the tables being commited. This could throw an exception
   * if the version is incorrect. This implies that the branch you are on is not up to date and there is a merge conflict.
   * </p>
   *
   * @param branch The branch to commit on. Its id is the commit version to commit on top of
   * @param tables list of tables to be added, deleted or modified
   */
  public void commit(Branch branch, Table... tables) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch.getName(), "multi"),
                                   MediaType.APPLICATION_JSON,
                                   checkKey())
                              .header(HttpHeaders.IF_MATCH, new EntityTag(branch.getId()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .put(Entity.entity(tables, MediaType.APPLICATION_JSON_TYPE));
    RestUtils.checkResponse(response);
  }

  /**
   * Return a list of all table names for a branch.
   *
   * <p>
   * We do not return all table objects as its a costly operation. Only table names. Optionally filtered by namespace
   * </p>
   */
  public Iterable<String> getAllTables(String branch, String namespace) {
    Response response = client.get(endpoint,
                                   SLASH.join("objects", branch, "tables"),
                                   MediaType.APPLICATION_JSON,
                                   checkKey(),
                                   ImmutableMap.of("namespace",
                                                   namespace == null ? "all" : namespace))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .get();
    RestUtils.checkResponse(response);
    return response.readEntity(new GenericType<List<String>>() {
    });
  }

  private static String extractHeaders(MultivaluedMap<String, Object> headers) {
    return (String) headers.getFirst(HttpHeaders.ETAG);
  }

  /**
   * Merge all commits from updateBranch onto branch. Optionally forcing.
   */
  public void assignBranch(com.dremio.nessie.model.Branch branch,
                           String updateBranchStr) {
    Table[] branchTable = new Table[0];
    Branch updateBranch = getBranch(updateBranchStr);
    Response response = client.get(endpoint,
                                   SLASH.join("objects",
                                              branch.getName()),
                                   MediaType.APPLICATION_JSON,
                                   checkKey(),
                                   ImmutableMap.of("target", updateBranch.getId()))
                              .header(HttpHeaders.IF_MATCH, new EntityTag(branch.getId()))
                              .accept(MediaType.APPLICATION_JSON_TYPE)
                              .put(Entity.entity(branchTable, MediaType.APPLICATION_JSON_TYPE));
    RestUtils.checkResponse(response);
  }

  /**
   * Delete a branch. Note this is potentially damaging if the branch is not fully merged.
   */
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

  public static NessieClient basic(String path, String username, String password) {
    return new NessieClient(AuthType.BASIC, path, username, password);
  }

  public static NessieClient aws(String path) {
    return new NessieClient(AuthType.AWS, path, null, null);
  }
}
