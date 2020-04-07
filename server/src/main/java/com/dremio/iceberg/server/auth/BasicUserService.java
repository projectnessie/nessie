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

import com.dremio.iceberg.auth.User;
import com.dremio.iceberg.auth.UserService;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

public class BasicUserService implements UserService {

  private static final Joiner JOINER = Joiner.on(',');
  private static final Map<String, String> USERS =
    ImmutableMap.of("admin_user", "test123", "plain_user", "hello123");
  private static final Map<String, Set<String>> ROLES = ImmutableMap.<String, Set<String>>builder()
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
    String expectedPassword = USERS.get(login);
    if (expectedPassword == null || !expectedPassword.equals(password)) {
      throw new NotAuthorizedException("User/password are incorrect");
    }
    return JwtUtils.issueToken(keyGenerator, JOINER.join(ROLES.get(login)), uriInfo, login);
  }

  @Override
  public User validate(String token) {
    Jws<Claims> claims = JwtUtils.checkToken(keyGenerator, token);

    return new User(new com.dremio.iceberg.model.User(claims.getBody().getSubject(),
                                                      null,
                                                      0,
                                                      true,
                                                      null,
                                                      Sets.newHashSet(((String) claims.getBody()
                                                                                      .get("roles"))
                                                                        .split(",")),
                                                      0
    ));
  }

  @Override
  public Optional<User> fetch(String username) {
    if (!USERS.containsKey(username)) {
      return Optional.empty();
    }

    return Optional.of(new User(new com.dremio.iceberg.model.User(username,
                                                                  null,
                                                                  0,
                                                                  true,
                                                                  null,
                                                                  ROLES.get(username),
                                                                  0)));
  }

  @Override
  public List<User> fetchAll() {
    return USERS.keySet().stream().map(u -> new User(new com.dremio.iceberg.model.User(u,
                                                                                       null,
                                                                                       0,
                                                                                       true,
                                                                                       null,
                                                                                       ROLES.get(u),
                                                                                       0
    ))).collect(Collectors.toList());
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
