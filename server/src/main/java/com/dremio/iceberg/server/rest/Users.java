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
package com.dremio.iceberg.server.rest;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.ServerConfiguration;
import com.dremio.iceberg.server.auth.Secured;
import com.dremio.iceberg.auth.UserService;

@Path("users")
public class Users {
  private static final Logger logger = LoggerFactory.getLogger(Users.class);
  private ServerConfiguration config;
  private UserService backend;

  @Inject
  public Users(ServerConfiguration config, UserService backend) {
    this.config = config;
    this.backend = backend;
  }

  @GET
  @Metered
  @ExceptionMetered(name="exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-create")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsers() {
    List<User> users = backend.fetchAll().stream().map(Users::toUser).collect(Collectors.toList());
    return Response.ok(new com.dremio.iceberg.model.Users(users)).build();
  }

  @GET
  @Path("{name}")
  @Metered
  @ExceptionMetered(name="exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-create")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsers(@PathParam("name") String name) {
    Optional<com.dremio.iceberg.auth.User> user = backend.fetch(name);
    if (user.isPresent()) {
      EntityTag eTag = new EntityTag(Integer.toString(user.hashCode()));
      return Response.ok(user).tag(eTag).build();
    } else {

      return Response.status(404, "user does not exist").build();
    }
  }

  @POST
  @Metered
  @ExceptionMetered(name="exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name="timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response addUser(User user) {
    try {
      Optional<com.dremio.iceberg.auth.User> userExisting = backend.fetchAll().stream()
        .filter(u -> u.getName().equals(user.getUsername()))
        .findFirst();
      if (userExisting.isPresent()) {
        return Response.status(409).build();
      }
      backend.create(fromUser(user));
      return Response.status(201).header(HttpHeaders.LOCATION, "users/" + user.getUsername()).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  //todo update and delete

  private static com.dremio.iceberg.auth.User fromUser(User user) {
    return new com.dremio.iceberg.auth.User(
      user.getUsername(),
      user.getRoles(),
      user.getPassword(),
      user.getCreateMillis(),
      user.isActive(),
      user.getExtraAttrs()
    );
  }

  private static User toUser(com.dremio.iceberg.auth.User user) {
    return new User(
      user.getName(),
      user.getPassword(),
      user.getCreateTime(),
      user.isActive(),
      null,
      user.getRoles(),
      user.getExtraAttrs()
    );
  }
}
