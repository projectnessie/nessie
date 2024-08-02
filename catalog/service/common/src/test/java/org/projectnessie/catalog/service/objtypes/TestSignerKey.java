/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.objtypes;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Instant;
import java.util.stream.Stream;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSignerKey {

  @ParameterizedTest
  @MethodSource
  public void signerKeyCheck(ImmutableSignerKey.Builder builder, String message) {
    assertThatIllegalStateException().isThrownBy(builder::build).withMessage(message);
  }

  @Test
  public void signerKeySpec() {
    Instant creation = Instant.now();
    Instant rotation = creation.plus(1, DAYS);
    Instant expiration = rotation.plus(1, DAYS);

    SignerKey key =
        SignerKey.builder()
            .name("key")
            .secretKey("01234567890123456789012345678901".getBytes(UTF_8))
            .creationTime(creation)
            .rotationTime(rotation)
            .expirationTime(expiration)
            .build();

    assertThat(key)
        .extracting(SignerKey::secretKeySpec)
        .isNotNull()
        .extracting(SecretKeySpec::getAlgorithm, SecretKeySpec::getFormat)
        .doesNotContainNull();
  }

  static Stream<Arguments> signerKeyCheck() {
    Instant creation = Instant.now();
    Instant rotation = creation.plus(1, DAYS);
    Instant expiration = rotation.plus(1, DAYS);

    byte[] secretKey = "01234567890123456789012345678901".getBytes(UTF_8);

    return Stream.of(
        arguments(
            SignerKey.builder()
                .name("key")
                .secretKey(secretKey)
                .creationTime(creation)
                .rotationTime(creation)
                .expirationTime(expiration),
            "creationTime must be before rotationTime"),
        arguments(
            SignerKey.builder()
                .name("key")
                .secretKey(secretKey)
                .creationTime(rotation)
                .rotationTime(creation)
                .expirationTime(expiration),
            "creationTime must be before rotationTime"),
        arguments(
            SignerKey.builder()
                .name("key")
                .secretKey(secretKey)
                .creationTime(creation)
                .rotationTime(rotation)
                .expirationTime(rotation),
            "rotationTime must be before expirationTime"),
        arguments(
            SignerKey.builder()
                .name("key")
                .secretKey(secretKey)
                .creationTime(creation)
                .rotationTime(expiration)
                .expirationTime(rotation),
            "rotationTime must be before expirationTime"),
        arguments(
            SignerKey.builder()
                .name("")
                .secretKey(secretKey)
                .creationTime(creation)
                .rotationTime(rotation)
                .expirationTime(expiration),
            "Key name must not be empty"),
        arguments(
            SignerKey.builder()
                .name("name")
                .secretKey("".getBytes(UTF_8))
                .creationTime(creation)
                .rotationTime(rotation)
                .expirationTime(expiration),
            "Secret key too short"),
        arguments(
            SignerKey.builder()
                .name("name")
                .secretKey("1234567".getBytes(UTF_8))
                .creationTime(creation)
                .rotationTime(rotation)
                .expirationTime(expiration),
            "Secret key too short"));
  }
}
