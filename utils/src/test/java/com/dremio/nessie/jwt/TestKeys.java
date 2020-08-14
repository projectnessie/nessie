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

package com.dremio.nessie.jwt;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwt;

public class TestKeys {

  @Test
  public void test() {
    KeyGenerator keyGenerator = new RandomKeyGenerator();
    String token = JwtUtils.issueToken(keyGenerator, "x,y,z", "http://example.com", "test");
    Jws<Claims> claims = JwtUtils.checkToken(keyGenerator, token);
    Assertions.assertEquals("test", claims.getBody().getSubject());
    Assertions.assertEquals("x,y,z", claims.getBody().get("roles", String.class));
    Jwt<Header, Claims> unsignedClaims = JwtUtils.checkToken(token);
    Assertions.assertEquals(claims.getBody().getExpiration(),
                            unsignedClaims.getBody().getExpiration());
  }

}
