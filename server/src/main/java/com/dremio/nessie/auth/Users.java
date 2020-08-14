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

package com.dremio.nessie.auth;

import java.time.ZoneId;
import java.time.ZonedDateTime;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.dremio.nessie.model.ImmutableUser;
import com.dremio.nessie.model.User;
import com.dremio.nessie.services.auth.Secured;

/**
 * endpoint for CRUD operations on Users.
 */
@Path("users")
public class Users {

  private final UserService backend;

  @Inject
  public Users(UserService backend) {
    this.backend = backend;
  }

  /**
   * Get all users.
   */
  @GET
  @Metered
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-create")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsers(@Context SecurityContext context) {
    if (context.isUserInRole("admin")) {
      List<User> users = backend.fetchAll()
                                .stream().map(Users::toUser).collect(Collectors.toList());
      return Response.ok(users.toArray(new User[]{})).build();
    } else {
      Optional<com.dremio.nessie.auth.User> user = backend.fetch(context.getUserPrincipal()
                                                                         .getName());
      if (!user.isPresent()) {
        return Response.status(404).build();
      }
      return Response.ok(new User[]{toUser(user.get())}).build();
    }
  }

  /**
   * Get a single user. Only valid if you are admin or you look for your own user.
   */
  @GET
  @Path("{name}")
  @Metered
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-create")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUsers(@PathParam("name") String name, @Context SecurityContext context) {
    if (!context.isUserInRole("admin") && !context.getUserPrincipal().getName().equals(name)) {
      return Response.status(403, "user not accessible").build();
    }
    Optional<com.dremio.nessie.auth.User> user = backend.fetch(name);
    if (user.isPresent()) {
      return Response.ok(Users.toUser(user.get())).build();
    } else {

      return Response.status(404, "user does not exist").build();
    }
  }

  /**
   * Create a user.
   */
  @POST
  @Metered
  @ExceptionMetered(name = "exception-create")
  @Secured
  @RolesAllowed({"admin"})
  @Timed(name = "timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response addUser(User user) {
    try {
      Optional<com.dremio.nessie.auth.User> userExisting = backend.fetchAll()
                                                                   .stream()
                                                                   .filter(u -> u.getName()
                                                                                 .equals(
                                                                                   user.getId()))
                                                                   .findFirst();
      if (userExisting.isPresent()) {
        return Response.status(409).build();
      }
      long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
      backend.create(Users.fromUser(ImmutableUser.builder()
                                                 .from(user)
                                                 .updateMillis(updateTime)
                                                 .build()));
      return Response.status(201).header(HttpHeaders.LOCATION, "users/" + user.getId()).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  /**
   * Modify a user, including changing its password for example.
   *
   * <p>
   *   A user can only modify their own account.
   * </p>
   */
  @PUT
  @Metered
  @Path("{name}")
  @ExceptionMetered(name = "exception-update")
  @Secured
  @RolesAllowed({"admin", "user"})
  @Timed(name = "timed-create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response modifyUser(@PathParam("name") String name, User user,
                             @Context SecurityContext context, @Context Request request) {
    if (!context.isUserInRole("admin") && !context.getUserPrincipal().getName().equals(name)) {
      return Response.status(403, "user not accessible").build();
    }
    Optional<com.dremio.nessie.auth.User> oldUser = backend.fetch(name);
    if (!oldUser.isPresent()) {
      return Response.status(404).build();
    }
    try {
      com.dremio.nessie.auth.User newUser = fromUser(user);
      String ok = null;
      if (!oldUser.get().getName().equals(newUser.getName())) {
        return Response.status(400, "Can't change username").build();
      }
      if (!oldUser.get().unwrap().getRoles().equals(newUser.unwrap().getRoles())) {
        ok = "roles";
      }
      if (oldUser.get().unwrap().isActive() != newUser.unwrap().isActive()) {
        ok = "active";
      }
      if (oldUser.get().unwrap().getCreateMillis() != newUser.unwrap().getCreateMillis()) {
        ok = "create time";
      }
      if (ok != null && !context.isUserInRole("admin")) {
        return Response.status(400, "user can't change " + ok).build();
      }

      long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();

      backend.update(Users.fromUser(ImmutableUser.builder()
                                                 .from(user)
                                                 .updateMillis(updateTime)
                                                 .build()));
      return Response.status(200).header(HttpHeaders.LOCATION, "users/" + user.getId()).build();
    } catch (Throwable t) {
      return Response.status(400, "something went wrong").build();
    }
  }

  /**
   * Delete a user. Admin only.
   */
  @DELETE
  @Metered
  @Path("{name}")
  @ExceptionMetered(name = "exception-delete")
  @Secured
  @RolesAllowed({"admin"})
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

  public static com.dremio.nessie.auth.User fromUser(User user) {
    return new com.dremio.nessie.auth.User(user);
  }

  public static User toUser(com.dremio.nessie.auth.User user) {
    return user.unwrap();
  }
}
