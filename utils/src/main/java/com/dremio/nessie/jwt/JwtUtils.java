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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.Key;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import javax.ws.rs.NotAuthorizedException;

/**
 * utility class for generating json web tokens.
 */
public final class JwtUtils {

  private JwtUtils() {
  }

  /**
   * Issue a token signed by KeyGenerator.
   *
   * <p>
   *   KeyGenerator could be external jwt source or internally generated key.
   * </p>
   *
   * @param keyGenerator private key with which to sign
   * @param roles roles that this jwt will embed
   * @param uri uri for the issuer
   * @param login username that this token is for
   * @return serialized jwt
   */
  public static String issueToken(KeyGenerator keyGenerator, String roles, String uri,
                                  String login) {
    Key key = keyGenerator.generateKey();
    String jwtToken = Jwts.builder()
                          .setSubject(login)
                          .setIssuer(uri)
                          .setIssuedAt(new Date())
                          .setExpiration(toDate(LocalDateTime.now().plusMinutes(15L)))
                          .signWith(key, SignatureAlgorithm.HS512)
                          .claim("roles", roles)
                          .compact();
    return jwtToken;
  }

  /**
   * Verify validity of given token.
   *
   * @param keyGenerator public key to decrypt the token
   * @param token given token
   * @return inflated JWT with claims attached
   */
  public static Jws<Claims> checkToken(KeyGenerator keyGenerator, String token) {
    try {
      Key key = keyGenerator.generateKey();
      JwtParser parser = Jwts.parserBuilder().setSigningKey(key).build();
      return parser.parseClaimsJws(token);
    } catch (Throwable t) {
      throw new NotAuthorizedException(t);
    }
  }

  /**
   * Parse token without validation.
   *
   * @param token given token
   * @return inflated JWT with claims attached
   */
  public static Jwt<Header, Claims> checkToken(String token) {
    try {
      JwtParser parser = Jwts.parserBuilder().build();
      int i = token.lastIndexOf('.');
      String withoutSignature = token.substring(0, i + 1);
      return parser.parseClaimsJwt(withoutSignature);
    } catch (Throwable t) {
      throw new NotAuthorizedException(t);
    }
  }

  private static Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }
}
