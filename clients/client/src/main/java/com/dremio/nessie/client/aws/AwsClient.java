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

package com.dremio.nessie.client.aws;

import com.dremio.nessie.client.BaseClient;
import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Table;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import javax.ws.rs.core.EntityTag;

public class AwsClient implements BaseClient {

  private final JsonApiGatewayCaller client;
  private final ObjectMapper mapper;

  public AwsClient(String endpoint) {
    client = new JsonApiGatewayCaller.Builder().endpoint(endpoint).buildClient();
    mapper = new ObjectMapperContextResolver().getContext(null);
  }

  @Override
  public NessieConfiguration getConfig() {
    try {
      NessieConfiguration result = client.fetchJson(GatewayOps.CONFIG, null, null, null);
      return result;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to create config object", e);
    }
  }

  @Override
  public Branch[] getBranches() {
    try {
      Branch[] result = client.fetchJson(GatewayOps.LIST_BRANCHES, null, null, null);
      return result;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to create config object", e);
    }
  }

  @Override
  public Table getTable(String branch,
                        String name,
                        String namespace) {
    return null;
  }

  @Override
  public Branch getBranch(String branchName) {
    try {
      Branch result = client.fetchJson(GatewayOps.GET_BRANCH, branchName, null, null);
      return result;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  @Override
  public Branch createBranch(Branch branch) {
    try {
      client.fetchJson(GatewayOps.CREATE_BRANCH,
                       branch.getName(),
                       mapper.writeValueAsString(branch),
                       null);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("unable to parse branch " + branch, e);
    }
    return getBranch(branch.getName());
  }

  @Override
  public void commit(Branch branch, Table... tables) {

  }

  @Override
  public String[] getAllTables(String branch, String namespace) {
    return new String[0];
  }

  @Override
  public void mergeBranch(Branch branch, String updateBranch, boolean force) {

  }

  @Override
  public void deleteBranch(Branch branch) {
    try {
      client.fetchJson(GatewayOps.DELETE_BRANCH,
                       branch.getName(),
                       mapper.writeValueAsString(branch),
                       new EntityTag(Objects.requireNonNull(branch.getId())));
    } catch (JsonProcessingException e) {
      //pass can't happen
    }
  }

  @Override
  public void close() throws Exception {

  }
}
