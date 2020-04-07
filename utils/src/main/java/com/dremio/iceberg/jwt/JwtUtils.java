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

package com.dremio.iceberg.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.Key;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.UriInfo;

/**
 * utility class for generating json web tokens.
 */
public final class JwtUtils {

  private JwtUtils() {
  }

  public static String issueToken(KeyGenerator keyGenerator, String roles, UriInfo uriInfo,
                                  String login) {
    Key key = keyGenerator.generateKey();
    String jwtToken = Jwts.builder()
                          .setSubject(login)
                          .setIssuer(uriInfo.getAbsolutePath().toString())
                          .setIssuedAt(new Date())
                          .setExpiration(toDate(LocalDateTime.now().plusMinutes(15L)))
                          .signWith(key, SignatureAlgorithm.HS512)
                          .claim("roles", roles)
                          .compact();
    return jwtToken;
  }

  public static Jws<Claims> checkToken(KeyGenerator keyGenerator, String token) {
    try {
      Key key = keyGenerator.generateKey();
      JwtParser parser = Jwts.parserBuilder().setSigningKey(key).build();
      return parser.parseClaimsJws(token);
    } catch (Throwable t) {
      throw new NotAuthorizedException(t);
    }
  }

  private static Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }
}
