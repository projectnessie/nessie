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

import com.dremio.iceberg.auth.Users;
import com.dremio.iceberg.json.ObjectMapperContextResolver;
import com.dremio.iceberg.model.ImmutableUser;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.auth.AlleyAuthFilter;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

public class RestLoginTest extends JerseyTest {

  private final String authHeader = "";

  @Override
  protected Application configure() {
    ResourceConfig rc = new ResourceConfig(Users.class);
    rc.register(new AlleyTestServerBinder());
    rc.register(AlleyAuthFilter.class);
    rc.register(RolesAllowedDynamicFeature.class);
    rc.register(ObjectMapperContextResolver.class);
    return rc;
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config);
    config.register(ObjectMapperContextResolver.class);
  }

  @Test
  public void testUser() {
    Response response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
                                       .header(HttpHeaders.AUTHORIZATION, authHeader)
                                       .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, response.readEntity(User[].class).length);
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
                                     .header(HttpHeaders.AUTHORIZATION, authHeader)
                                     .get();
    User user = response.readEntity(User.class);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("normal", user.getId());
    Assert.assertEquals(1, user.getRoles().size());
    Assert.assertEquals("user", new ArrayList<>(user.getRoles()).get(0));
    User newUser = getUser("bob", "123", false);
    response = target("users").request(MediaType.APPLICATION_JSON)
                              .header(HttpHeaders.AUTHORIZATION, authHeader)
                              .post(Entity.entity(newUser, MediaType.APPLICATION_JSON));
    Assert.assertEquals(201, response.getStatus());
    response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
                              .header(HttpHeaders.AUTHORIZATION, authHeader)
                              .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(3, response.readEntity(User[].class).length);

    response = target("users/bob").request(MediaType.APPLICATION_JSON_TYPE)
                                  .header(HttpHeaders.AUTHORIZATION, authHeader)
                                  .delete();
    Assert.assertEquals(200, response.getStatus());
    response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
                              .header(HttpHeaders.AUTHORIZATION, authHeader)
                              .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, response.readEntity(User[].class).length);

  }

  @Test
  public void userSecurity() {
    Response response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
                                       .header(HttpHeaders.AUTHORIZATION, "normal")
                                       .get();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(1, response.readEntity(User[].class).length);
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
                                     .header(HttpHeaders.AUTHORIZATION, "normal")
                                     .get();
    User user = response.readEntity(User.class);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("normal", user.getId());
    Assert.assertEquals(1, user.getRoles().size());
    Assert.assertEquals("user", new ArrayList<>(user.getRoles()).get(0));

    response = target("users/foo").request(MediaType.APPLICATION_JSON_TYPE)
                                  .header(HttpHeaders.AUTHORIZATION, "normal")
                                  .get();
    Assert.assertEquals(403, response.getStatus());

    response = target("users/test").request(MediaType.APPLICATION_JSON_TYPE)
                                   .header(HttpHeaders.AUTHORIZATION, "normal")
                                   .put(Entity.entity(getUser("test", "", true),
                                     MediaType.APPLICATION_JSON));
    Assert.assertEquals(403, response.getStatus());
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
                                     .header(HttpHeaders.AUTHORIZATION, "normal")
                                     .put(Entity.entity(getUser("normal", "123", false),
                                                        MediaType.APPLICATION_JSON));
    Assert.assertEquals(200, response.getStatus());
    response = target("users/normal").request(MediaType.APPLICATION_JSON_TYPE)
                                     .header(HttpHeaders.AUTHORIZATION, "normal")
                                     .put(Entity.entity(getUser("normal", "", true),
                                                        MediaType.APPLICATION_JSON));
    Assert.assertEquals(400, response.getStatus());

    response = target("users/test").request(MediaType.APPLICATION_JSON_TYPE)
                                   .header(HttpHeaders.AUTHORIZATION, "normal")
                                   .delete();
    Assert.assertEquals(403, response.getStatus());

    response = target("users").request(MediaType.APPLICATION_JSON_TYPE)
                              .header(HttpHeaders.AUTHORIZATION, "normal")
                              .post(Entity.entity(getUser("normal", "", true),
                                                  MediaType.APPLICATION_JSON));
    Assert.assertEquals(403, response.getStatus());
  }

  private User getUser(String name, String password, boolean admin) {
    return ImmutableUser.builder().id(name)
                        .password(password)
                        .addAllRoles(
                          admin ? ImmutableSet.of("admin", "user") : ImmutableSet.of("user"))
                        .build();
  }
}
