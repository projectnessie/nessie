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

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

import com.dremio.nessie.client.auth.Auth;
import com.dremio.nessie.client.auth.BasicAuth;
import com.dremio.nessie.client.auth.NoAuth;
import com.dremio.nessie.client.rest.NessieNotFoundException;
import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;

public class NessieService implements Closeable {

  public enum AuthType {
    AWS,
    BASIC,
    NONE
  }

  static {
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  private final Auth auth;
  private final ResteasyClient client;
  private final NessieSimpleClient nessie;

  /**
   * create new nessie client. All REST api endpoints are mapped here.
   *
   * @param path URL for the nessie client (eg http://localhost:19120/api/v1)
   */
  public NessieService(AuthType authType, String path, String username, String password) {
    client = new ResteasyClientBuilderImpl().register(ObjectMapperContextResolver.class).build();
    ResteasyWebTarget target = client.target(path);
    nessie = target.proxy(NessieSimpleClient.class);
    switch (authType) {
      case AWS:
        auth = null;
        break;
      case BASIC:
        auth = new BasicAuth(username, password, nessie);
        break;
      case NONE:
        auth = new NoAuth();
        break;
      default:
        throw new IllegalStateException(String.format("%s does not exist", authType));
    }
  }

  private String checkKey() {
    return auth == null ? null : auth.checkKey();
  }

  /**
   * Fetch configuration from the server.
   */
  public NessieConfiguration getConfig() {
    Response response = nessie.getConfig(checkKey());
    RestUtils.checkResponse(response);
    return response.readEntity(NessieConfiguration.class);
  }

  /**
   * Fetch all known branches from the server.
   */
  public Iterable<ReferenceWithType<Reference>> getBranches() {
    Response response = nessie.refs(checkKey());
    RestUtils.checkResponse(response);
    return response.readEntity(new GenericType<List<ReferenceWithType<Reference>>>() {
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
    Response response = nessie.refTable(branch, table, false, checkKey());
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
    Response response = nessie.ref(branchName, checkKey());
    try {
      RestUtils.checkResponse(response);
    } catch (NessieNotFoundException e) {
      return null;
    }
    String pair = extractHeaders(response.getHeaders());
    ReferenceWithType<Branch> branch = response.readEntity(new GenericType<ReferenceWithType<Branch>>() {
    });
    assert branch.getReference().getId().equals(pair.replaceAll("\"", ""));
    return branch.getReference();
  }

  /**
   * Create a new branch. Branch name is the branch name and id is the branch to copy from.
   */
  public Branch createBranch(Branch branch) {
    Response response = nessie.createRef(branch.getName(), branch.getId(), checkKey(), ReferenceWithType.of(branch));
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
    Response response = nessie.updateMulti(branch.getName(), null, branch.getId(), checkKey(), tables);
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
    Response response = nessie.refTables(branch, namespace, checkKey());
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
    //Table[] branchTable = new Table[0];
    Branch updateBranch = getBranch(updateBranchStr);
    Response response = nessie.updateBatch(branch.getId(), updateBranch.getId(), branch.getId(), checkKey());
    RestUtils.checkResponse(response);
  }

  /**
   * Delete a branch. Note this is potentially damaging if the branch is not fully merged.
   */
  public void deleteBranch(Branch branch) {
    Response response = nessie.deleteRef(branch.getName(), branch.getId(), checkKey());
    RestUtils.checkResponse(response);
  }

  public static NessieService basic(String path, String username, String password) {
    return new NessieService(AuthType.BASIC, path, username, password);
  }

  public static NessieService aws(String path) {
    return new NessieService(AuthType.AWS, path, null, null);
  }

  public static void main(String[] args) {
    basic("http://localhost:19120/api/v1", "admin_user", "test123").createBranch(ImmutableBranch.builder().name("main").build());
    System.out.println(basic("http://localhost:19120/api/v1", "admin_user", "test123").getBranches());
  }
}
