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

import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

import com.dremio.nessie.client.auth.AuthFilter;
import com.dremio.nessie.client.rest.NessieNotFoundException;
import com.dremio.nessie.client.rest.ObjectMapperContextResolver;
import com.dremio.nessie.client.rest.ResponseCheckFilter;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Contents;

import io.opentracing.contrib.jaxrs2.client.ClientTracingFeature;

public class NessieClient implements Closeable {

  public enum AuthType {
    AWS,
    BASIC,
    NONE
  }

  static {
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
  }

  private final ResteasyClient client;
  private final NessieClientDefinition nessie;

  /**
   * create new nessie client. All REST api endpoints are mapped here.
   *
   * @param path URL for the nessie client (eg http://localhost:19120/api/v1)
   */
  public NessieClient(AuthType authType, String path, String username, String password) {

    client = new ResteasyClientBuilderImpl().register(ObjectMapperContextResolver.class)
                                            .register(ClientTracingFeature.class)
                                            .register(ResponseCheckFilter.class)
                                            .build();
    ResteasyWebTarget target = client.target(path);
    AuthFilter authFilter = new AuthFilter(authType, username, password, target);
    client.register(authFilter);
    nessie = target.proxy(NessieClientDefinition.class);
  }

  /**
   * Fetch configuration from the server.
   */
  public NessieConfiguration getConfig() {
    return nessie.getConfig();
  }

  /**
   * Fetch all known branches from the server.
   */
  public Iterable<ReferenceWithType<Reference>> getBranches() {
    return nessie.refs();
  }

  @Override
  public void close() {
    client.close();
  }

  /**
   * Get a single table specific to a given branch.
   */
  public Contents getTable(String branch, String name, String namespace) {
    String table = (namespace == null) ? name : (namespace + "." + name);
    try {
      return nessie.refTable(branch, table, false);
    } catch (NessieNotFoundException e) {
      return null;
    }
  }

  /**
   * Get a branch for a given name.
   */
  public Branch getBranch(String branchName) {
    try {
      ReferenceWithType<Branch> branch = nessie.ref(branchName);
      return branch.getReference();
    } catch (NessieNotFoundException e) {
      return null;
    }
  }

  /**
   * Create a new branch. Branch name is the branch name and id is the branch to copy from.
   */
  public Branch createBranch(Branch branch) {
    nessie.createRef(branch.getName(), branch.getId(), ReferenceWithType.of(branch));
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
  public void commit(Branch branch, Contents... tables) {
    nessie.updateMulti(branch.getName(), null, branch.getId(), tables);
  }

  /**
   * Return a list of all table names for a branch.
   *
   * <p>
   * We do not return all table objects as its a costly operation. Only table names. Optionally filtered by namespace
   * </p>
   */
  public Iterable<String> getAllTables(String branch, String namespace) {
    return nessie.refTables(branch, namespace);
  }

  /**
   * Merge all commits from updateBranch onto branch.
   */
  public void assignBranch(com.dremio.nessie.model.Branch branch,
                           String updateBranchStr) {
    Branch updateBranch = getBranch(updateBranchStr);
    nessie.updateBatch(branch.getName(), updateBranch.getId(), branch.getId());
  }

  /**
   * Delete a branch. Note this is potentially damaging if the branch is not fully merged.
   */
  public void deleteBranch(Branch branch) {
    nessie.deleteRef(branch.getName(), branch.getId());
  }

  public static NessieClient basic(String path, String username, String password) {
    return new NessieClient(AuthType.BASIC, path, username, password);
  }

  public static NessieClient aws(String path) {
    return new NessieClient(AuthType.AWS, path, null, null);
  }

}
