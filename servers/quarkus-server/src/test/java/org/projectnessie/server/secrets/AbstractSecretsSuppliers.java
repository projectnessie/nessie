/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.server.secrets;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;

import jakarta.inject.Inject;
import java.net.URI;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.TokenSecret;

public abstract class AbstractSecretsSuppliers {

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  @SecretsUpdater SecretsUpdateHandler secretsUpdateHandler;
  @Inject SecretsProvider secretsProvider;

  protected abstract String providerName();

  protected URI buildSecretURN(String secretName) {
    return URI.create(format("urn:nessie-secret:%s:%s", providerName(), secretName));
  }

  @AfterEach
  public void tearDown() {
    // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
    // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
    soft.assertAll();
  }

  @ParameterizedTest
  @MethodSource
  public void writeAndRead(Map<String, Secret> secrets, Map<String, SecretType> secretTypes) {
    secretsUpdateHandler.updateSecrets(secrets, secretTypes);

    Function<String, Consumer<Secret>> conditionFor =
        name -> {
          Secret secret = secrets.get(name);
          if (secret instanceof KeySecret key) {
            return s -> assertThat((KeySecret) s).extracting(KeySecret::key).isEqualTo(key.key());
          } else if (secret instanceof BasicCredentials basic) {
            return s ->
                assertThat((BasicCredentials) s)
                    .extracting(BasicCredentials::name, BasicCredentials::secret)
                    .containsExactly(basic.name(), basic.secret());
          } else if (secret instanceof TokenSecret token) {
            return s ->
                assertThat((TokenSecret) s)
                    .extracting(TokenSecret::token, TokenSecret::expiresAt)
                    .containsExactly(token.token(), token.expiresAt());
          } else {
            throw new RuntimeException("Unsupported secret type: " + secret);
          }
        };

    Map<String, Secret> resolvedAll =
        secretTypes.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        secretsProvider
                            .getSecret(buildSecretURN(e.getKey()), e.getValue(), Secret.class)
                            .orElseThrow()));

    soft.assertThat(resolvedAll).hasSize(secrets.size()).hasSize(secretTypes.size());
    secrets.forEach(
        (name, secret) ->
            soft.assertThat(resolvedAll)
                .describedAs(name)
                .extracting(name, type(Secret.class))
                .satisfies(conditionFor.apply(name)));

    secrets.forEach(
        (k, v) ->
            soft.assertThat(
                    secretsProvider.getSecret(buildSecretURN(k), secretTypes.get(k), Secret.class))
                .get()
                .satisfies(conditionFor.apply(k)));
  }

  static Stream<Arguments> writeAndRead() {
    return Stream.of(
        arguments(Map.of("single", keySecret("that-secret")), Map.of("single", SecretType.KEY)),
        arguments(Map.of("mine", basicCredentials("foo", "bar")), Map.of("mine", SecretType.BASIC)),
        arguments(
            Map.of(
                //
                "one", basicCredentials("a", "b"),
                "two", basicCredentials("x", "y")),
            Map.of("one", SecretType.BASIC, "two", SecretType.BASIC)));
  }
}
