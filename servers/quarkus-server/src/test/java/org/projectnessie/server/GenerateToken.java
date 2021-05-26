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
package org.projectnessie.server;

import io.smallrye.jwt.build.Jwt;
import java.util.Arrays;
import java.util.HashSet;
import org.eclipse.microprofile.jwt.Claims;

public class GenerateToken {

  /** Generate JWT token. */
  public static void main(String[] args) {
    String token =
        Jwt.issuer("http://nessie.dremio.com")
            .upn("admin_user")
            .groups(new HashSet<>(Arrays.asList("user", "admin")))
            .claim(Claims.birthdate.name(), "2001-07-13")
            .expiresAt(Long.MAX_VALUE)
            .sign();
    System.out.println(token);
    token =
        Jwt.issuer("https://quarkus.io/using-jwt-rbac")
            .upn("test_user")
            .groups(new HashSet<>(Arrays.asList("user")))
            .claim(Claims.birthdate.name(), "2001-07-13")
            .expiresAt(Long.MAX_VALUE)
            .sign();
    System.out.println(token);
  }
}
