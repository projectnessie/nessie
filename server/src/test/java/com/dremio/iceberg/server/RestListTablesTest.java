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

package com.dremio.iceberg.server;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;
import com.dremio.iceberg.server.rest.ListTables;
import com.dremio.iceberg.server.rest.Login;

public class RestListTablesTest extends JerseyTest {

  private String authHeader = "";

  @Override
  protected Application configure() {
    ResourceConfig rc = new ResourceConfig(ListTables.class);
    rc.register(new AlleyServerBinder());
    return rc;
  }

  //todo test put and delete
  @Test
  public void tablesTest() {
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .post(Entity.entity(new Table("test1", "loc"), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());
    Table table = target("tables/test1").request().get(Table.class);
    Assert.assertEquals(new Table("test1", "loc"), table);
    Tables tables = target("tables").request().get(Tables.class);
    Assert.assertEquals(1, tables.getTables().size());
    Assert.assertEquals(table, tables.getTables().get(0));
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void duplicateTest() {
    // create new entity
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .post(Entity.entity(new Table("test2", "loc"), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());

    // create same entity again...fail
    response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .post(Entity.entity(new Table("test2", "loc"), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(400, response.getStatus());

    // update entity
    response = target("tables/test2").request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .put(Entity.entity(new Table("test2", "loc2"), MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());
    Table table = target("tables/test2").request().get(Table.class);
    Assert.assertEquals("loc2", table.getBaseLocation());

    // delete entity
    response = target("tables/test2").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .delete();
    Assert.assertEquals(200, response.getStatus());
    try {
      target("tables/test2").request().get(Table.class);
      Assert.fail();
    } catch (NotFoundException e) {
      //pass
    }

    // delete entity that doesn't exist...fail
    response = target("tables/test2").request(MediaType.APPLICATION_JSON_TYPE).delete();
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void optimisticLocking() {
    // create new entity
    Response response = target("tables").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .post(Entity.entity(new Table("test3", "loc"), MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(201, response.getStatus());

    // get entity for user 1
    Response responseU1 = target("tables/test3").request().get();
    Table tableU1 = responseU1.readEntity(Table.class);

    // get entity for user 2
    Response responseU2 = target("tables/test3").request().get();
    Table tableU2 = responseU2.readEntity(Table.class);

    //u1 modifty and commit
    tableU1.setMetadataLocation("foobar");
    response = target("tables/test3").request(MediaType.APPLICATION_JSON)
      .header("If-Match", responseU1.getHeaders().getFirst("ETag"))
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .put(Entity.entity(tableU1, MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());

    //u2 modify and commit...will fail
    tableU2.setMetadataLocation("foobaz");
    response = target("tables/test3").request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .header("If-Match", responseU2.getHeaders().getFirst("ETag"))
      .put(Entity.entity(tableU2, MediaType.APPLICATION_JSON));
    Assert.assertEquals(412, response.getStatus());

    //u2 refresh and try again...will succeed
    responseU2 = target("tables/test3").request().get();
    tableU2 = responseU2.readEntity(Table.class);
    response = target("tables/test3").request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .header("If-Match", responseU2.getHeaders().getFirst("ETag"))
      .put(Entity.entity(tableU2, MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());
  }
}
