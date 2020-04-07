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

import java.security.SecureRandom;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

import com.dremio.iceberg.auth.User;
import com.dremio.iceberg.auth.UserService;
import com.google.common.base.Joiner;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;

public class DatabaseUserService implements UserService {
  private static final Joiner JOINER = Joiner.on(",");
  @Context
  private UriInfo uriInfo;
  @Inject
  UserServiceDbBackend backend;
  @Inject
  KeyGenerator keyGenerator;


  @Override
  public String authorize(String login, String password) {
    com.dremio.iceberg.model.User user = backend.get(login);
    if (user == null) {
      throw new NotAuthorizedException("user/password are incorrect");
    }
    String expectedPassword = user.getPassword();
    if (check(password, expectedPassword)) {
      throw new NotAuthorizedException("User/password are incorrect");
    }
    return JwtUtils.issueToken(keyGenerator, JOINER.join(user.getRoles()), uriInfo, login);
  }

  @Override
  public User validate(String token) {
    Jws<Claims> claims = JwtUtils.checkToken(keyGenerator, token);
    com.dremio.iceberg.model.User user = backend.get(claims.getBody().getSubject());
    if (user == null || !user.isActive()) {
      throw new NotAuthorizedException("{} does not exist", claims.getBody().getSubject());
    }
    return fromUser(user);
  }

  private static com.dremio.iceberg.auth.User fromUser(com.dremio.iceberg.model.User user) {
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

  private static com.dremio.iceberg.model.User toUser(com.dremio.iceberg.auth.User user) {
    return new com.dremio.iceberg.model.User(
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
  @Override
  public Optional<User> fetch(String username) {
    com.dremio.iceberg.model.User user = backend.get(username);
    if (user != null) {
      return Optional.of(fromUser(user));
    }
    return Optional.empty();
  }

  @Override
  public List<User> fetchAll() {
    return backend.getAll(false).stream().map(DatabaseUserService::fromUser).collect(Collectors.toList());
  }

  @Override
  public void create(User user) {
    com.dremio.iceberg.model.User modelUser = toUser(user);
    String hashedPassword = hash(modelUser.getPassword());
    modelUser.setPassword(hashedPassword);
    backend.create(user.getName(), modelUser);
  }

  @Override
  public void update(User user) {
    com.dremio.iceberg.model.User modelUser = toUser(user);
    com.dremio.iceberg.model.User oldUser = backend.get(user.getName());
    if (!check(modelUser.getPassword(), oldUser.getPassword())) {
      String hashedPassword = hash(modelUser.getPassword());
      modelUser.setPassword(hashedPassword);
    }
    backend.update(user.getName(), modelUser);
  }

  @Override
  public void delete(String user) {
    backend.remove(user);
  }

  private String hash(String password) {
    SecureRandom random = new SecureRandom();
    byte[] salt = random.generateSeed(16);
    return OpenBSDBCrypt.generate(password.toCharArray(), salt, 16);
  }

  private boolean check(String password, String hash) {
    return OpenBSDBCrypt.checkPassword(hash, password.toCharArray());
  }
}
