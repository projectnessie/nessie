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

package com.dremio.nessie.server;

import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.ImmutableTag;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.services.rest.TableBranchOperations;

public class RestGitTest extends JerseyTest {

  private String authHeader = "";

  @Override
  protected Application configure() {
    ResourceConfig rc = new ResourceConfig(TableBranchOperations.class);
    rc.register(new NessieTestServerBinder());
    rc.register(ObjectMapperContextResolver.class);
    return rc;
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
    config.register(ObjectMapperContextResolver.class);
  }

  @Test
  public void testBasic() {
    ReferenceWithType<Branch> main = ReferenceWithType.of(ImmutableBranch.builder()
                                                      .id(null)
                                                      .name("main")
                                                      .build());
    Response res = get("objects/main").post(Entity.entity(main, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());
    Branch[] branches = get().get(Branch[].class);
    Assertions.assertEquals(1, branches.length);
    Assertions.assertEquals("main", branches[0].getName());

    main = get("objects/main").get(new GenericType<ReferenceWithType<Branch>>(){});
    Assertions.assertEquals(branches[0], main.getReference());

    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(main.getReference().getId())
                                 .name("test")
                                 .build());
    res = get("objects/test").post(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());
    ReferenceWithType<Branch> testReturn = get("objects/test").get(new GenericType<ReferenceWithType<Branch>>(){});
    Assertions.assertEquals("test", testReturn.getReference().getName());

    Table table = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation("/the/directory/over/there")
                                .build();
    res = get("objects/test").get();
    res = get("objects/test/xxx.test").header("If-Match", res.getHeaders().getFirst("ETag"))
                                      .post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());

    Table[] updates = new Table[11];
    for (int i = 0; i < 10; i++) {
      updates[i] = ImmutableTable.builder()
                                 .id("xxx.test" + i)
                                 .name("test" + i)
                                 .namespace("xxx")
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build();
    }
    updates[10] = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation(
                                        "/the/directory/over/there/has/been/moved")
                                .build();
    res = get("objects/test").get();
    res = get("objects/test/multi").header("If-Match", res.getHeaders().getFirst("ETag"))
                             .put(Entity.entity(updates, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(200, res.getStatus());
    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation(
                                  "/the/directory/over/there/has/been/moved/again")
                          .build();
    res = get("objects/test/xxx.test").get();
    Assertions.assertEquals(updates[10], res.readEntity(Table.class));
    res = get("objects/test").get();
    res = get("objects/test/xxx.test").header("If-Match", res.getHeaders().getFirst("ETag"))
                                      .put(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(200, res.getStatus());
    res = get("objects/test/xxx.test").get();
    Assertions.assertEquals(table, res.readEntity(Table.class));

    String branchId = get("objects/test").get().readEntity(ReferenceWithType.class).getReference().getId();
    res = get("objects/tagtest").post(Entity.entity(ReferenceWithType.of(ImmutableTag.builder()
                                                                .name("tagtest")
                                                                .id(branchId)
                                                                .build()),
                                                    MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());
    res = get("objects/tagtest").get();
    Assertions.assertEquals(branchId, res.readEntity(ReferenceWithType.class).getReference().getId());
    res = get("objects/tagtest/multi").header("If-Match", new EntityTag(branchId))
                                      .put(Entity.entity(updates, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(404, res.getStatus());
    res = get("objects/tagtest").delete();
    Assertions.assertEquals(412, res.getStatus());
    res = get("objects/tagtest").header("If-Match", new EntityTag(branchId)).delete();
    Assertions.assertEquals(200, res.getStatus());

    res = get("objects/test/log").get();
    Assertions.assertEquals(200, res.getStatus());
    Map<String, CommitMeta> logs = res.readEntity(new GenericType<Map<String, CommitMeta>>(){});
    Assertions.assertEquals(4, logs.size());
    Assertions.assertEquals(13, logs.values().stream().mapToInt(CommitMeta::changes).sum());
  }

  @Test
  public void testNewBranch() {
    ReferenceWithType<Branch> main = ReferenceWithType.of(ImmutableBranch.builder()
                                                                         .id(null)
                                                                         .name("main")
                                                                         .build());
    Response res = get("objects/main").post(Entity.entity(main, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());
    Table[] updates = new Table[10];
    for (int i = 0; i < 10; i++) {
      updates[i] = ImmutableTable.builder()
                                 .id("xxx.test" + i)
                                 .name("test" + i)
                                 .namespace("xxx")
                                 .metadataLocation("/the/directory/over/there/" + i)
                                 .build();
    }
    res = get("objects/main").get();
    res = get("objects/main/multi").header("If-Match", res.getHeaders().getFirst("ETag"))
                               .put(Entity.entity(updates, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(200, res.getStatus());
    res = get("objects/main").get();
    String hash = res.readEntity(ReferenceWithType.class).getReference().getId();
    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(hash)
                                 .name("test2")
                                 .build());
    res = get("objects/test2").post(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());

    for (int i = 0; i < 10; i++) {
      Table bt = updates[i];
      res = get("objects/test2/" + bt.getId()).get();
      Assertions.assertEquals(200, res.getStatus());
      Assertions.assertEquals(bt, res.readEntity(Table.class));
    }

    res = get("objects/test2").get();
    res = get("objects/test2/" + updates[0].getId()).header("If-Match",
                                                            res.getHeaders().getFirst("ETag"))
                                                    .delete();
    Assertions.assertEquals(200, res.getStatus());
    res = get("objects/test2/" + updates[0].getId()).get();
    Assertions.assertEquals(404, res.getStatus());
    res = get("objects/test2").get();
    res = get("objects/test2").header("If-Match", res.getHeaders().getFirst("ETag"))
                              .delete();
    Assertions.assertEquals(200, res.getStatus());
    res = get("objects/test2").get();
    Assertions.assertEquals(404, res.getStatus());

  }

  @SuppressWarnings("LocalVariableName")
  @Test
  public void testOptimisticLocking() {
    ReferenceWithType<Branch> test = ReferenceWithType.of(ImmutableBranch.builder()
                                 .id(null)
                                 .name("test3")
                                 .build());
    Response res = get("objects/test3").post(Entity.entity(test, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());
    Table table = ImmutableTable.builder()
                                .id("xxx.test")
                                .name("test")
                                .namespace("xxx")
                                .metadataLocation("/the/directory/over/there")
                                .build();
    res = get("objects/test3").get();
    res = get("objects/test3/xxx.test").header("If-Match", res.getHeaders().getFirst("ETag"))
                                       .post(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(201, res.getStatus());

    Object eTagStart = get("objects/test3").get().getHeaders().getFirst("ETag");
    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation("/the/directory/over/there/has/been/moved")
                          .build();
    res = get("objects/test3/xxx.test").header("If-Match", eTagStart)
                                       .put(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(200, res.getStatus());
    table = ImmutableTable.builder()
                          .id("xxx.test")
                          .name("test")
                          .namespace("xxx")
                          .metadataLocation("/the/directory/over/there/has/been/moved/again")
                          .build();
    res = get("objects/test3/xxx.test").header("If-Match", eTagStart)
                                       .put(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(412, res.getStatus());
    Object eTagNew = get("objects/test3").get().getHeaders().getFirst("ETag");
    res = get("objects/test3/xxx.test").header("If-Match", eTagNew)
                                       .put(Entity.entity(table, MediaType.APPLICATION_JSON_TYPE));
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
