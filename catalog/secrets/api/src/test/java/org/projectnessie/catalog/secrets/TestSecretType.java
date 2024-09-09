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
package org.projectnessie.catalog.secrets;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSecretType {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void keySecret() {
    soft.assertThat(SecretType.KEY.parse("foo"))
        .extracting(KeySecret.class::cast)
        .extracting(KeySecret::key)
        .isEqualTo("foo");
    soft.assertThat(SecretType.KEY.parse("{\"key\": \"foo\"}"))
        .extracting(KeySecret.class::cast)
        .extracting(KeySecret::key)
        .isEqualTo("{\"key\": \"foo\"}");
    soft.assertThat(SecretType.KEY.parse("{\"value\": \"foo\"}"))
        .extracting(KeySecret.class::cast)
        .extracting(KeySecret::key)
        .isEqualTo("{\"value\": \"foo\"}");

    soft.assertThat(SecretType.KEY.fromValueMap(Map.of("key", "foo")))
        .extracting(KeySecret.class::cast)
        .extracting(KeySecret::key)
        .isEqualTo("foo");
    soft.assertThat(SecretType.KEY.fromValueMap(Map.of("value", "foo")))
        .extracting(KeySecret.class::cast)
        .extracting(KeySecret::key)
        .isEqualTo("foo");
    soft.assertThat(SecretType.KEY.fromValueMap(Map.of("blah", "foo"))).isNull();
    soft.assertThat(SecretType.KEY.fromValueMap(Map.of())).isNull();
  }

  @Test
  public void tokenSecret() {
    String instantStr = "2024-12-24T12:12:12Z";
    Instant instant = Instant.parse(instantStr);

    soft.assertThat(SecretType.EXPIRING_TOKEN.parse("tok"))
        .extracting(TokenSecret.class::cast)
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly("tok", Optional.empty());
    soft.assertThat(SecretType.EXPIRING_TOKEN.parse("{\"value\": \"tok\"}"))
        .extracting(TokenSecret.class::cast)
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly("tok", Optional.empty());
    soft.assertThat(
            SecretType.EXPIRING_TOKEN.parse(
                "{\"value\": \"tok\", \"expiresAt\": \"" + instantStr + "\"}"))
        .extracting(TokenSecret.class::cast)
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly("tok", Optional.of(instant));
    soft.assertThat(SecretType.EXPIRING_TOKEN.parse("{\"blah\": \"foo\"}")).isNull();
    soft.assertThat(SecretType.EXPIRING_TOKEN.parse("{}")).isNull();

    soft.assertThat(SecretType.EXPIRING_TOKEN.fromValueMap(Map.of("token", "tok")))
        .extracting(TokenSecret.class::cast)
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly("tok", Optional.empty());
    soft.assertThat(SecretType.EXPIRING_TOKEN.fromValueMap(Map.of("value", "tok")))
        .extracting(TokenSecret.class::cast)
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly("tok", Optional.empty());
    soft.assertThat(
            SecretType.EXPIRING_TOKEN.fromValueMap(Map.of("token", "tok", "expiresAt", instantStr)))
        .extracting(TokenSecret.class::cast)
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly("tok", Optional.of(instant));
    soft.assertThat(SecretType.EXPIRING_TOKEN.fromValueMap(Map.of("blah", "foo"))).isNull();
    soft.assertThat(SecretType.EXPIRING_TOKEN.fromValueMap(Map.of())).isNull();
  }

  @Test
  public void basicCredential() {
    soft.assertThat(SecretType.BASIC.parse("tok")).isNull();
    soft.assertThat(SecretType.BASIC.parse("{\"name\": \"user\", \"secret\": \"pass\"}"))
        .extracting(BasicCredentials.class::cast)
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("user", "pass");
    soft.assertThat(SecretType.BASIC.parse("{\"blah\": \"foo\"}")).isNull();
    soft.assertThat(SecretType.BASIC.parse("{}")).isNull();

    soft.assertThat(SecretType.BASIC.fromValueMap(Map.of("name", "user", "secret", "pass")))
        .extracting(BasicCredentials.class::cast)
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("user", "pass");
    soft.assertThat(SecretType.BASIC.fromValueMap(Map.of("name", "user"))).isNull();
    soft.assertThat(SecretType.BASIC.fromValueMap(Map.of("blah", "foo"))).isNull();
    soft.assertThat(SecretType.BASIC.fromValueMap(Map.of())).isNull();
  }
}
