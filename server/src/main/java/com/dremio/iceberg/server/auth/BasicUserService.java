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
package com.dremio.iceberg.server.auth;

import java.security.Key;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

public class BasicUserService implements UserService {
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
    Key key = keyGenerator.generateKey();
    String jwtToken = Jwts.builder()
        .setSubject(login)
        .setIssuer(uriInfo.getAbsolutePath().toString())
        .setIssuedAt(new Date())
        .setExpiration(toDate(LocalDateTime.now().plusMinutes(15L)))
        .signWith(key, SignatureAlgorithm.HS512)
        .claim("roles", Joiner.on(',').join(roles.get(login)))
        .compact();
    return jwtToken;
  }

  @Override
  public User validate(String token) {
    try {
      Key key = keyGenerator.generateKey();
      JwtParser parser = Jwts.parserBuilder().setSigningKey(key).build();
      Jws<Claims> claims = parser.parseClaimsJws(token);
      return new User(claims.getBody().getSubject(), (String) claims.getBody().get("roles"));
    } catch (Throwable t) {
      throw new NotAuthorizedException(t);
    }
  }

  private Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }
}
