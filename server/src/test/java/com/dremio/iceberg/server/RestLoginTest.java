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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.internal.guava.Lists;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.auth.AlleyAuthFilter;
import com.dremio.iceberg.server.rest.Users;
import com.google.common.collect.ImmutableSet;

public class RestLoginTest extends JerseyTest {

  private String authHeader = "";

  @Override
  protected Application configure() {
    ResourceConfig rc = new ResourceConfig(Users.class);
    rc.register(new AlleyTestServerBinder());
    rc.register(AlleyAuthFilter.class);
    rc.register(RolesAllowedDynamicFeature.class);
    return rc;
  }

  @Test
  public void testUser() {
    Response response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, response.readEntity(com.dremio.iceberg.model.Users.class).getUsers().size());
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .get();
    User user = response.readEntity(User.class);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("normal", user.getUsername());
    Assert.assertEquals(1, user.getRoles().size());
    Assert.assertEquals("user", Lists.newArrayList(user.getRoles()).get(0));
    User newUser = new User("bob", "bob",
      "123",
      0,
      true,
      null,
      ImmutableSet.of("user"),
      null,
      0L);
    response = target("users").request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .post(Entity.entity(newUser, MediaType.APPLICATION_JSON));
    Assert.assertEquals(201, response.getStatus());
    response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(3, response.readEntity(com.dremio.iceberg.model.Users.class).getUsers().size());

    response = target("users/bob").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .delete();
    Assert.assertEquals(200, response.getStatus());
    response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, response.readEntity(com.dremio.iceberg.model.Users.class).getUsers().size());

  }

  @Test
  public void userSecurity() {
    Response response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(1, response.readEntity(com.dremio.iceberg.model.Users.class).getUsers().size());
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .get();
    User user = response.readEntity(User.class);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("normal", user.getUsername());
    Assert.assertEquals(1, user.getRoles().size());
    Assert.assertEquals("user", Lists.newArrayList(user.getRoles()).get(0));

    response = target("users/foo").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .get();
    Assert.assertEquals(403, response.getStatus());

    response = target("users/test").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .put(Entity.entity(
        new User("test", null, 0, true, null,
          ImmutableSet.of("admin", "user"), 1L, 0L), MediaType.APPLICATION_JSON));
    Assert.assertEquals(403, response.getStatus());
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .put(Entity.entity(
        new User("normal", "123", 0, true, null,
          ImmutableSet.of("user"), 1L, 0L), MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .put(Entity.entity(
        new User("normal", null, 0, true, null,
          ImmutableSet.of("admin", "user"), 1L, 0L), MediaType.APPLICATION_JSON));
    Assert.assertEquals(400, response.getStatus());

    response = target("users/test").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .delete();
    Assert.assertEquals(403, response.getStatus());

    response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
      .header(HttpHeaders.AUTHORIZATION, "normal")
      .post(Entity.entity(
        new User("normal", null, 0, true, null,
          ImmutableSet.of("admin", "user"), 1L, 0L), MediaType.APPLICATION_JSON));
    Assert.assertEquals(403, response.getStatus());
  }

  @Test
  public void optimisticLocking() {
    // get entity for user 1
    Response responseU1 = target("users/test").request()
      .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    User tableU1 = responseU1.readEntity(User.class);

    // get entity for user 2
    Response responseU2 = target("users/test").request()
      .header(HttpHeaders.AUTHORIZATION, authHeader).get();
    User tableU2 = responseU2.readEntity(User.class);

    //u1 modifty and commit
    tableU1.setEmail("foobar");
    Response response = target("users/test").request(MediaType.APPLICATION_JSON)
      .header("If-Match", responseU1.getHeaders().getFirst("ETag"))
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .put(Entity.entity(tableU1, MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());

    //u2 modify and commit...will fail
    tableU2.setEmail("foobaz");
    response = target("users/test").request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .header("If-Match", responseU2.getHeaders().getFirst("ETag"))
      .put(Entity.entity(tableU2, MediaType.APPLICATION_JSON));
    Assert.assertEquals(412, response.getStatus());

    //u2 refresh and try again...will succeed
    responseU2 = target("users/test").request().header(HttpHeaders.AUTHORIZATION, authHeader).get();
    tableU2 = responseU2.readEntity(User.class);
    response = target("users/test").request(MediaType.APPLICATION_JSON)
      .header(HttpHeaders.AUTHORIZATION, authHeader)
      .header("If-Match", responseU2.getHeaders().getFirst("ETag"))
      .put(Entity.entity(tableU2, MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());

  }
}
