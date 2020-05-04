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

package com.dremio.iceberg.server;

import com.dremio.iceberg.backend.dynamodb.LocalDynamoDB;
import com.dremio.iceberg.backend.dynamodb.model.GitObject;
import com.dremio.iceberg.json.ObjectMapperContextResolver;
import com.dremio.iceberg.model.Branch;
import com.dremio.iceberg.model.BranchTable;
import com.dremio.iceberg.model.ImmutableBranch;
import com.dremio.iceberg.model.ImmutableBranchTable;
import com.dremio.iceberg.server.rest.TableBranch;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.jgit.lib.Constants;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;

public class RestGitTest extends JerseyTest {
  private static final LocalDynamoDB SERVER = new LocalDynamoDB();

  private String authHeader = "";

  @BeforeClass
  public static void start() throws Exception {
    SERVER.start();
    DynamoDbEnhancedClient ec = DynamoDbEnhancedClient.builder()
                                                      .dynamoDbClient(SERVER.client())
                                                      .build();
    BeanTableSchema<GitObject> schema = TableSchema.fromBean(GitObject.class);
    DynamoDbTable<GitObject> table = ec.table(
      "NessieGitObjectDatabase",
      schema);
    table.createTable();

  }

  @AfterClass
  public static void stop() throws Exception {
    SERVER.close();
  }

  @Override
  protected Application configure() {
    ResourceConfig rc = new ResourceConfig(TableBranch.class);
    rc.register(new AlleyTestServerBinder("dynamo"));
    rc.register(ObjectMapperContextResolver.class);
    return rc;
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
    config.register(ObjectMapperContextResolver.class);
  }

  @Test
  public void testNamespace() {
    Branch[] branches = get().get(Branch[].class);
    Assertions.assertEquals(1, branches.length);
    Assertions.assertEquals(Constants.MASTER, branches[0].getId());

    Branch master = get("objects/master").get(Branch.class);
    Assertions.assertEquals(branches[0], master);

    Branch test = ImmutableBranch.builder()
                                 .id("test")
                                 .build();
    Response res = get("objects/test").post(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(200, res.getStatus());
    Branch testReturn = get("objects/test").get(Branch.class);
    Assertions.assertEquals(testReturn, test);

    BranchTable table = ImmutableBranchTable.builder()
      .id("xxx.test")
      .tableName("test")
      .namespace("xxx")
      .metadataLocation("/the/directory/over/there")
      .build();
    res = get("objects/test/xxx.test").post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(200, res.getStatus());
  }

  private Builder get() {
    return get("objects");
  }

  private Builder get(String endpoint) {
    return target(endpoint).request(MediaType.APPLICATION_JSON_TYPE)
                     .header(HttpHeaders.AUTHORIZATION, authHeader);
  }
}
