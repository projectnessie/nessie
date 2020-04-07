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
package com.dremio.iceberg.server.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import com.dremio.iceberg.auth.User;
import com.dremio.iceberg.auth.UserService;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;

public class BasicUserService implements UserService {
  private static final Joiner JOINER = Joiner.on(',');
  private static final Map<String, String> users =
      ImmutableMap.of("admin_user", "test123", "plain_user", "hello123");
  private static final Map<String, Set<String>> roles = ImmutableMap.<String, Set<String>>builder()
      .put("admin_user", ImmutableSet.of("admin"))
      .put("plain_user", ImmutableSet.of("user"))
      .build();

  @Inject
  private KeyGenerator keyGenerator;
  @Context
  private UriInfo uriInfo;

  @Override
  public String authorize(String login, String password) {
    return issueToken(login, password);
  }

  private String issueToken(String login, String password) {
    String expectedPassword = users.get(login);
    if (expectedPassword == null || !expectedPassword.equals(password)) {
      throw new NotAuthorizedException("User/password are incorrect");
    }
    return JwtUtils.issueToken(keyGenerator, JOINER.join(roles.get(login)), uriInfo, login);
  }

  @Override
  public User validate(String token) {
      Jws<Claims> claims = JwtUtils.checkToken(keyGenerator, token);
      return new User(claims.getBody().getSubject(), (String) claims.getBody().get("roles"));
  }

  @Override
  public Optional<User> fetch(String username) {
    if (!users.containsKey(username)) {
      return Optional.empty();
    }

    return Optional.of(new User(username, JOINER.join(roles.get(username))));
  }

  @Override
  public List<User> fetchAll() {
    return users.keySet().stream().map(u -> new User(u, JOINER.join(roles.get(u)))).collect(Collectors.toList());
  }

  @Override
  public void create(User user) {
    throw new UnsupportedOperationException("Can't add users to simple user service");
  }

  @Override
  public void update(User user) {
    throw new UnsupportedOperationException("Cant update users in simple user service");
  }

  @Override
  public void delete(String user) {
    throw new UnsupportedOperationException("Can't delete users in simple user service");
  }

}
