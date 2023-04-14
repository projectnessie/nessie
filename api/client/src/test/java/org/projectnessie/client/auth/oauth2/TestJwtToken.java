/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestJwtToken {

  /** Valid empty JWT token. */
  public static final String JWT_EMPTY =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
          + ".e30"
          + "._rLw7_14xQJEMXf0l0HOvLuy7WYP6_zjLbMYsTlU0tM";

  /**
   * Valid non-empty JWT token.
   *
   * <pre>
   * {
   * "iss": "Alice",
   * "sub": "Bob",
   * "aud": "Charlie",
   * "exp": 1621634624,
   * "nbf": 1621634625,
   * "iat": 1621634626,
   * "jti": "Nessie"
   * }
   * </pre>
   */
  static final String JWT_NON_EMPTY =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
          + ".eyJpc3MiOiJBbGljZSIsInN1YiI6IkJvYiIsImF1ZCI6IkNoYXJsaWUiLCJleHAiOjE2MjE2MzQ2MjQsIm5iZiI6MTYyMTYzNDYyNSwiaWF0IjoxNjIxNjM0NjI2LCJqdGkiOiJOZXNzaWUifQ"
          + ".IMCmFN8Ysk0Dsm8kb0q8ksh6FLfjL3EaqnyYJGjkNwU";

  static final Instant JWT_EXP_CLAIM = Instant.ofEpochSecond(1621634624);
  static final Instant JWT_NBF_CLAIM = Instant.ofEpochSecond(1621634625);
  static final Instant JWT_IAT_CLAIM = Instant.ofEpochSecond(1621634626);

  @ParameterizedTest
  @MethodSource
  void testJwtIssuer(String token, boolean valid, String expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getIssuer()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtIssuer() {
    return Stream.concat(commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, "Alice")));
  }

  @ParameterizedTest
  @MethodSource
  void testJwtSubject(String token, boolean valid, String expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getSubject()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtSubject() {
    return Stream.concat(commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, "Bob")));
  }

  @ParameterizedTest
  @MethodSource
  void testJwtAudience(String token, boolean valid, String expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getAudience()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtAudience() {
    return Stream.concat(commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, "Charlie")));
  }

  @ParameterizedTest
  @MethodSource
  void testJwtExpirationTime(String token, boolean valid, Instant expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getExpirationTime()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtExpirationTime() {
    return Stream.concat(
        commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, JWT_EXP_CLAIM)));
  }

  @ParameterizedTest
  @MethodSource
  void testJwtNotBefore(String token, boolean valid, Instant expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getNotBefore()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtNotBefore() {
    return Stream.concat(
        commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, JWT_NBF_CLAIM)));
  }

  @ParameterizedTest
  @MethodSource
  void testJwtIssuedAt(String token, boolean valid, Instant expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getIssuedAt()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtIssuedAt() {
    return Stream.concat(
        commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, JWT_IAT_CLAIM)));
  }

  @ParameterizedTest
  @MethodSource
  void testJwtId(String token, boolean valid, String expected) {
    if (valid) {
      JwtToken jwtToken = JwtToken.parse(token);
      assertThat(jwtToken).isNotNull();
      assertThat(jwtToken.getId()).isEqualTo(expected);
    } else {
      assertThatThrownBy(() -> JwtToken.parse(token))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid JWT token: " + token);
    }
  }

  static Stream<Arguments> testJwtId() {
    return Stream.concat(commonCases(), Stream.of(Arguments.of(JWT_NON_EMPTY, true, "Nessie")));
  }

  private static Stream<Arguments> commonCases() {
    return Stream.of(
        Arguments.of(null, false, null),
        Arguments.of("", false, null),
        Arguments.of("invalidtoken", false, null),
        Arguments.of("invalid.token", false, null),
        Arguments.of("invalid.to.ken", false, null),
        Arguments.of("invalid..token", false, null),
        Arguments.of("in.valid.to.ken", false, null),
        Arguments.of(JWT_EMPTY, true, null));
  }
}
