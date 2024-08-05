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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.Instant;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSignerKeysObj {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void signerKeyByName() {
    Instant creation = Instant.now();
    Instant rotation = creation.plus(1, DAYS);
    Instant expiration = rotation.plus(1, DAYS);

    SignerKey key1 =
        ImmutableSignerKey.of(
            "key1",
            "01234567890123456789012345678901".getBytes(UTF_8),
            creation,
            rotation,
            expiration);
    SignerKey key2 =
        ImmutableSignerKey.of(
            "key2",
            "01234567890123456789012345678902".getBytes(UTF_8),
            creation,
            rotation,
            expiration);
    SignerKey key3 =
        ImmutableSignerKey.of(
            "key3",
            "01234567890123456789012345678903".getBytes(UTF_8),
            creation,
            rotation,
            expiration);
    SignerKeysObj signerKeys =
        SignerKeysObj.builder().versionToken("foo").addSignerKeys(key1, key2, key3).build();

    soft.assertThat(signerKeys.getSignerKey("key1")).isEqualTo(key1);
    soft.assertThat(signerKeys.getSignerKey("key2")).isEqualTo(key2);
    soft.assertThat(signerKeys.getSignerKey("key3")).isEqualTo(key3);
    soft.assertThat(signerKeys.getSignerKey("nah")).isNull();
  }

  @ParameterizedTest
  @MethodSource
  public void signerKeysCheck(ImmutableSignerKeysObj.Builder builder, String message) {
    assertThatIllegalStateException().isThrownBy(builder::build).withMessage(message);
  }

  static Stream<Arguments> signerKeysCheck() {
    Instant creation = Instant.now();
    Instant rotation = creation.plus(1, DAYS);
    Instant expiration = rotation.plus(1, DAYS);

    SignerKey key1 =
        ImmutableSignerKey.of(
            "key1",
            "01234567890123456789012345678901".getBytes(UTF_8),
            creation,
            rotation,
            expiration);
    SignerKey key1b =
        ImmutableSignerKey.of(
            "key1",
            "01234567890123456789012345678902".getBytes(UTF_8),
            creation,
            rotation,
            expiration);

    return Stream.of(
        arguments(
            SignerKeysObj.builder().versionToken("ver"), "At least one signer-key is required"),
        arguments(
            SignerKeysObj.builder().versionToken("ver").addSignerKeys(key1, key1b),
            "Duplicate signer key names"));
  }
}
