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

package com.dremio.nessie.services.auth;

import java.util.Map;
import java.util.Set;

import javax.ws.rs.NotAuthorizedException;

import org.eclipse.microprofile.jwt.Claims;

import com.dremio.nessie.auth.LoginService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.smallrye.jwt.build.Jwt;

/**
 * User service for testing only.
 */
public class BasicUserService implements LoginService {

  private static final Map<String, String> USERS =
      ImmutableMap.of("admin_user", "test123", "plain_user", "hello123");
  private static final Map<String, Set<String>> ROLES = ImmutableMap.<String, Set<String>>builder()
      .put("admin_user", ImmutableSet.of("admin"))
      .put("plain_user", ImmutableSet.of("user"))
      .build();

  @Override
  public String authorize(String login, String password) {
    return issueToken(login, password);
  }

  private String issueToken(String login, String password) {
    String expectedPassword = USERS.get(login);
    if (expectedPassword == null || !expectedPassword.equals(password)) {
      throw new NotAuthorizedException("User/password are incorrect");
    }
    return Jwt.issuer("https://nessie.dremio.com")
              .upn(login)
              .groups(ROLES.get(login))
              .claim(Claims.birthdate.name(), "2001-07-13")
              .sign();
  }

}
