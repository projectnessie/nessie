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

import static org.projectnessie.catalog.secrets.SecretType.BASIC;
import static org.projectnessie.catalog.secrets.SecretType.KEY;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import java.net.URI;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestResolvingSecretsProvider {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void multipleProviders() {
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager(
                "one",
                unsafePlainTextSecretsProvider(
                    Map.of("basic", Map.of("name", "the-name", "secret", "the-secret"))))
            .putSecretsManager(
                "two", unsafePlainTextSecretsProvider(Map.of("key", Map.of("key", "key-value"))))
            .build();

    soft.assertThat(
            secretsProvider.getSecret(
                URI.create("urn:nessie-secret:one:basic"), BASIC, BasicCredentials.class))
        .get()
        .isInstanceOf(BasicCredentials.class)
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("the-name", "the-secret");
    soft.assertThat(
            secretsProvider.getSecret(
                URI.create("urn:nessie-secret:two:key"), KEY, KeySecret.class))
        .get()
        .isInstanceOf(KeySecret.class)
        .extracting(KeySecret::key)
        .isEqualTo("key-value");
    soft.assertThat(
            secretsProvider.getSecret(
                URI.create("urn:nessie-secret:no-provider:key"), KEY, KeySecret.class))
        .isEmpty();
  }

  @ParameterizedTest
  @CsvSource({
    "foo",
    "foo:nessie-secret:provider:key",
    "urn",
    // "urn:", <-- illegal URI
    "urn::",
    "urn:::",
    "urn:secret::",
    "urn:secret:provider:key",
    "urn:secret:provider:key",
    "urn:nessie-secret",
    "urn::nessie-secret",
    "urn:nessie-secret:",
    "urn:nessie-secret:provider",
    "urn:nessie-secret::provider",
    "urn::nessie-secret::provider",
    "urn:nessie-secret:provider:",
    "urn:nessie-secret:provider::key",
  })
  public void illegal(String name) {
    SecretsProvider secretsProvider = ResolvingSecretsProvider.builder().build();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> secretsProvider.getSecret(URI.create(name), BASIC, BasicCredentials.class))
        .withMessage(
            "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");
  }
}
