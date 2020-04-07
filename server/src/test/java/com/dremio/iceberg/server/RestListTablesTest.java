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

import com.dremio.iceberg.client.TableClient;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.VersionedWrapper;
import com.dremio.iceberg.server.rest.ListTables;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RestListTablesTest extends JerseyTest {

  private String authHeader = "";

  @Override
  protected Application configure() {
    ResourceConfig rc = new ResourceConfig(ListTables.class);
    rc.register(new AlleyTestServerBinder());
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
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
                                        .header(HttpHeaders.AUTHORIZATION, authHeader)
                                        .post(Entity.entity(Table.builder()
                                                                 .tableName("test1")
                                                                 .namespace("a.b.c")
                                                                 .baseLocation("loc")
                                                                 .metadataLocation("xxx")
                                                                 .build(),
                                                            MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());
    String tableName = response.getLocation().getRawPath();
    VersionedWrapper<Table> table = target(tableName).request().get(TableClient.TABLE_TYPE);
    Assert.assertNotNull(table);
    VersionedWrapper<Table> tableByName = target("tables/by-name/test1")
        .queryParam("namespace", "a.b.c")
        .request(MediaType.APPLICATION_JSON)
        .header(HttpHeaders.AUTHORIZATION, authHeader)
        .get(TableClient.TABLE_TYPE);
    Assert.assertEquals(tableByName.getObj(), table.getObj());
    response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
                               .header(HttpHeaders.AUTHORIZATION, authHeader)
                               .post(Entity.entity(Table.builder()
                                                        .tableName("test1")
                                                        .baseLocation("loc")
                                                        .metadataLocation("xxx")
                                                        .build(),
                                                   MediaType.APPLICATION_JSON_TYPE));
    tableName = response.getLocation().getRawPath();
    Table[] tables = target("tables").queryParam("namespace", "a.b.c")
                                     .request(MediaType.APPLICATION_JSON)
                                     .header(HttpHeaders.AUTHORIZATION, authHeader)
                                     .get(Table[].class);
    Assert.assertEquals(1, tables.length);
    Assert.assertEquals("a.b.c", tables[0].getNamespace());
    response = target(tableName).request(MediaType.APPLICATION_JSON)
                                .header(HttpHeaders.AUTHORIZATION, authHeader)
                                .delete();
    Assert.assertEquals(200, response.getStatus());
    response = target("tables/" + table.getObj().getId()).request(MediaType.APPLICATION_JSON)
                                                         .header(HttpHeaders.AUTHORIZATION,
                                                                 authHeader)
                                                         .delete();
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void tablesTest() {
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
                                        .header(HttpHeaders.AUTHORIZATION, authHeader)
                                        .post(Entity.entity(Table.builder()
                                                                 .tableName("test1")
                                                                 .baseLocation("loc")
                                                                 .metadataLocation("xxx")
                                                                 .build(),
                                                            MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());
    String tableName = response.getLocation().getRawPath();
    VersionedWrapper<Table> table = target(tableName).request().get(TableClient.TABLE_TYPE);
    Assert.assertEquals(table.getObj().getTableName(), "test1");
    Table[] tables = target("tables").request().get(Table[].class);
    Assert.assertEquals(1, tables.length);
    Assert.assertEquals(table.getObj(), tables[0]);
    target(tableName).request().delete();
    tables = target("tables").request().get(Table[].class);
    Assert.assertEquals(0, tables.length);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void duplicateTest() {
    // create new entity
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
                                        .header(HttpHeaders.AUTHORIZATION, authHeader)
                                        .post(Entity.entity(Table.builder()
                                                                 .tableName("test2")
                                                                 .baseLocation("loc")
                                                                 .metadataLocation("xxx")
                                                                 .build(),
                                                            MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());
    String tableName = response.getLocation().getRawPath();
    VersionedWrapper<Table> table = target(tableName).request(MediaType.APPLICATION_JSON)
                                                     .header(HttpHeaders.AUTHORIZATION, authHeader)
                                                     .get(TableClient.TABLE_TYPE);

    // create same entity again...fail
    response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
                               .header(HttpHeaders.AUTHORIZATION, authHeader)
                               .post(Entity.entity(Table.builder()
                                                        .tableName("test2")
                                                        .baseLocation("loc")
                                                        .metadataLocation("xxx")
                                                        .build(),
                                                   MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(409, response.getStatus());

    // update entity
    Table newTable = Table.copyTable(table.getObj()).baseLocation("loc2").build();
    response = target(tableName).request(MediaType.APPLICATION_JSON)
                                .header(HttpHeaders.AUTHORIZATION, authHeader)
                                .put(Entity.entity(newTable, MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());
    table = target(tableName).request().get(TableClient.TABLE_TYPE);
    Assert.assertEquals("loc2", table.getObj().getBaseLocation());

    // delete entity
    response = target(tableName).request(MediaType.APPLICATION_JSON_TYPE)
                                .header(HttpHeaders.AUTHORIZATION, authHeader)
                                .delete();
    Assert.assertEquals(200, response.getStatus());
    try {
      target(tableName).request().get(TableClient.TABLE_TYPE);
      Assert.fail();
    } catch (NotFoundException e) {
      //pass
    } catch (Throwable t) {
      Assert.fail();
    }

    // delete entity that doesn't exist...fail
    response = target(tableName).request(MediaType.APPLICATION_JSON_TYPE).delete();
    Assert.assertEquals(404, response.getStatus());
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void optimisticLocking() {
    // create new entity
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
                                        .header(HttpHeaders.AUTHORIZATION, authHeader)
                                        .post(Entity.entity(Table.builder()
                                                                 .tableName("test3")
                                                                 .baseLocation("loc")
                                                                 .metadataLocation("xxx")
                                                                 .build(),
                                                            MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());
    String tableName = response.getLocation().getRawPath();

    // get entity for user 1
    Response responseU1 = target(tableName).request().accept(MediaType.APPLICATION_JSON_TYPE).get();
    VersionedWrapper<Table> tableU1 = responseU1.readEntity(TableClient.TABLE_TYPE);

    // get entity for user 2
    Response responseU2 = target(tableName).request().accept(MediaType.APPLICATION_JSON_TYPE).get();
    VersionedWrapper<Table> tableU2 = responseU2.readEntity(TableClient.TABLE_TYPE);

    //u1 modifty and commit
    tableU1 = tableU1.update(Table.copyTable(tableU1.getObj()).metadataLocation("foobar").build());
    response = target(tableName).request(MediaType.APPLICATION_JSON)
                                .header("If-Match", responseU1.getHeaders().getFirst("ETag"))
                                .header(HttpHeaders.AUTHORIZATION, authHeader)
                                .put(Entity.entity(tableU1.getObj(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());

    //u2 modify and commit...will fail
    tableU2 = tableU2.update(Table.copyTable(tableU2.getObj()).metadataLocation("foobaz").build());
    response = target(tableName).request(MediaType.APPLICATION_JSON)
                                .header(HttpHeaders.AUTHORIZATION, authHeader)
                                .header("If-Match", responseU2.getHeaders().getFirst("ETag"))
                                .put(Entity.entity(tableU2.getObj(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(412, response.getStatus());

    //u2 refresh and try again...will succeed
    responseU2 = target(tableName)
      .request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .accept(MediaType.APPLICATION_JSON_TYPE)
      .get();
    tableU2 = responseU2.readEntity(TableClient.TABLE_TYPE);
    response = target(tableName).request(MediaType.APPLICATION_JSON)
                                .header(HttpHeaders.AUTHORIZATION, authHeader)
                                .header("If-Match", responseU2.getHeaders().getFirst("ETag"))
                                .put(Entity.entity(tableU2.getObj(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());
    target(tableName).request().delete();
    Table[] tables =
      target("tables").request().accept(MediaType.APPLICATION_JSON_TYPE).get(Table[].class);
    Assert.assertEquals(0, tables.length);

  }
}
