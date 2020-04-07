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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.iceberg.auth.UserService;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.server.ServerConfiguration;
import com.dremio.iceberg.server.auth.Secured;
import com.google.common.collect.ImmutableList;

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
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed( {"admin", "user"})
  @Timed(name = "timed-create")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsers(@Context SecurityContext context) {
    if (context.isUserInRole("admin")) {
      List<User> users = backend.fetchAll().stream().map(Users::toUser).collect(Collectors.toList());
      return Response.ok(new com.dremio.iceberg.model.Users(users)).build();
    } else {
      Optional<com.dremio.iceberg.auth.User> user = backend.fetch(context.getUserPrincipal().getName());
      if (!user.isPresent()) {
        return Response.status(404).build();
      }
      return Response.ok(new com.dremio.iceberg.model.Users(ImmutableList.of(toUser(user.get())))).build();
    }
  }

  @GET
  @Path("{name}")
  @Metered
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed( {"admin", "user"})
  @Timed(name = "timed-create")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsers(@PathParam("name") String name, @Context SecurityContext context) {
    if (!context.isUserInRole("admin") && !context.getUserPrincipal().getName().equals(name)) {
      return Response.status(403, "user not accessible").build();
    }
    Optional<com.dremio.iceberg.auth.User> user = backend.fetch(name);
    if (user.isPresent()) {
      return Response.ok(Users.toUser(user.get())).tag(new EntityTag(user.get().getVersion().toString())).build();
    } else {

      return Response.status(404, "user does not exist").build();
    }
  }

  @POST
  @Metered
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed( {"admin"})
  @Timed(name = "timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response addUser(User user) {
    try {
      Optional<com.dremio.iceberg.auth.User> userExisting = backend.fetchAll().stream()
        .filter(u -> u.getName().equals(user.getUsername()))
        .findFirst();
      if (userExisting.isPresent()) {
        return Response.status(409).build();
      }
      backend.create(Users.fromUser(user));
      return Response.status(201).header(HttpHeaders.LOCATION, "users/" + user.getUsername()).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  @PUT
  @Metered
  @Path("{name}")
  @ExceptionMetered(name = "exception-update")
  @Secured
  @RolesAllowed( {"admin", "user"})
  @Timed(name = "timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response modifyUser(@PathParam("name") String name, User user, @Context SecurityContext context, @Context Request request) {
    if (!context.isUserInRole("admin") && !context.getUserPrincipal().getName().equals(name)) {
      return Response.status(403, "user not accessible").build();
    }
    Optional<com.dremio.iceberg.auth.User> oldUser = backend.fetch(name);
    if (!oldUser.isPresent()) {
      return Response.status(404).build();
    }
    EntityTag eTag = new EntityTag(oldUser.get().getVersion().toString());
    Response.ResponseBuilder evaluationResultBuilder = request.evaluatePreconditions(eTag);
    if (evaluationResultBuilder != null) {
      return evaluationResultBuilder.status(412, "Tag not up to date").build();
    }
    try {
      com.dremio.iceberg.auth.User newUser = fromUser(user);
      String ok = null;
      if (!oldUser.get().getName().equals(newUser.getName())) {
        return Response.status(400, "Can't change username").build();
      }
      if (!oldUser.get().getRoles().equals(newUser.getRoles())) {
        ok = "roles";
      }
      if (oldUser.get().isActive() != newUser.isActive()) {
        ok = "active";
      }
      if (oldUser.get().getCreateTime() != newUser.getCreateTime()) {
        ok = "create time";
      }
      if (ok != null && !context.isUserInRole("admin")) {
        return Response.status(400, "user can't change " + ok).build();
      }
      backend.update(Users.fromUser(user));
      return Response.status(200).header(HttpHeaders.LOCATION, "users/" + user.getUsername()).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  @DELETE
  @Metered
  @Path("{name}")
  @ExceptionMetered(name = "exception-delete")
  @Secured
  @RolesAllowed( {"admin"})
  @Timed(name = "timed-delete")
  public Response deleteUser(@PathParam("name") String name, @Context SecurityContext context) {
    if (!context.isUserInRole("admin") && !context.getUserPrincipal().getName().equals(name)) {
      return Response.status(403, "user not accessible").build();
    }
    try {
      backend.delete(name);
      return Response.status(200).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  private static com.dremio.iceberg.auth.User fromUser(User user) {
    return new com.dremio.iceberg.auth.User(
      user.getUsername(),
      user.getRoles(),
      user.getPassword(),
      user.getCreateMillis(),
      user.isActive(),
      user.getVersion(),
      user.getUpdateMillis()
    );
  }

  private static User toUser(com.dremio.iceberg.auth.User user) {
    return new User(
      user.getName(),
      user.getName(),
      user.getPassword(),
      user.getCreateTime(),
      user.isActive(),
      null,
      user.getRoles(),
      user.getVersion(),
      user.getUpdateTime()
    );
  }
}
