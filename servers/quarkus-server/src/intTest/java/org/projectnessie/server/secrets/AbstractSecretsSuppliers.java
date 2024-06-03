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
package org.projectnessie.server.secrets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.SecretAttribute.secretAttribute;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretAttribute;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.catalog.secrets.spi.SecretsSupplier;
import org.projectnessie.nessie.immutables.NessieImmutable;

public abstract class AbstractSecretsSuppliers {

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  @SecretsUpdater SecretsUpdateHandler secretsUpdateHandler;
  @Inject SecretsSupplier secretsSupplier;
  @Inject SecretsProvider secretsProvider;

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

    Map<String, Secret> resolvedAll = secretsSupplier.resolveSecrets(secretTypes);

    soft.assertThat(resolvedAll).hasSize(secrets.size()).hasSize(secretTypes.size());
    secrets.forEach(
        (name, secret) ->
            soft.assertThat(resolvedAll)
                .describedAs(name)
                .extracting(name, type(Secret.class))
                .satisfies(conditionFor.apply(name)));

    secrets.forEach(
        (k, v) ->
            soft.assertThat(secretsSupplier.resolveSecrets(Map.of(k, secretTypes.get(k))))
                .hasSize(1)
                .hasEntrySatisfying(k, conditionFor.apply(k)));
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

  @ParameterizedTest
  @MethodSource
  public void checkProvider(String name, Opts opts) {
    KeySecret keySecret = opts.key().orElseThrow();
    BasicCredentials basicCredentials = opts.basic().orElseThrow();
    TokenSecret tokenSecret = opts.expiringToken().orElseThrow();

    Map<String, Secret> secrets =
        Map.of(
            "base." + name + ".key",
            keySecret,
            "base." + name + ".basic",
            basicCredentials,
            "base." + name + ".expiringToken",
            tokenSecret);
    Map<String, SecretType> secretTypes =
        Map.of(
            "base." + name + ".key",
            SecretType.KEY,
            "base." + name + ".basic",
            SecretType.BASIC,
            "base." + name + ".expiringToken",
            SecretType.EXPIRING_TOKEN);

    secretsUpdateHandler.updateSecrets(secrets, secretTypes);

    soft.assertThat(secretsSupplier.resolveSecrets(secretTypes))
        .hasSize(3)
        .hasEntrySatisfying(
            "base." + name + ".key",
            s ->
                assertThat(s)
                    .asInstanceOf(type(KeySecret.class))
                    .extracting(KeySecret::key)
                    .isEqualTo(keySecret.key()))
        .hasEntrySatisfying(
            "base." + name + ".basic",
            s ->
                assertThat(s)
                    .asInstanceOf(type(BasicCredentials.class))
                    .extracting(BasicCredentials::name, BasicCredentials::secret)
                    .containsExactly(basicCredentials.name(), basicCredentials.secret()))
        .hasEntrySatisfying(
            "base." + name + ".expiringToken",
            s ->
                assertThat(s)
                    .asInstanceOf(type(TokenSecret.class))
                    .extracting(TokenSecret::token, TokenSecret::expiresAt)
                    .containsExactly(tokenSecret.token(), tokenSecret.expiresAt()));

    Opts.Builder builder = Opts.builder();

    Opts retrieved =
        secretsProvider
            .applySecrets(builder, "base", Opts.EMPTY, name, Opts.EMPTY, providerAttributes())
            .build();

    soft.assertThat(retrieved.basic())
        .get()
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly(basicCredentials.name(), basicCredentials.secret());
    soft.assertThat(retrieved.key()).get().extracting(KeySecret::key).isEqualTo(keySecret.key());
    soft.assertThat(retrieved.expiringToken())
        .get()
        .extracting(TokenSecret::token, TokenSecret::expiresAt)
        .containsExactly(tokenSecret.token(), tokenSecret.expiresAt());
  }

  static List<SecretAttribute<Opts, Opts.Builder, ?>> providerAttributes() {
    return List.of(
        secretAttribute("key", SecretType.KEY, Opts::key, Opts.Builder::key),
        secretAttribute(
            "expiringToken",
            SecretType.EXPIRING_TOKEN,
            Opts::expiringToken,
            Opts.Builder::expiringToken),
        secretAttribute("basic", SecretType.BASIC, Opts::basic, Opts.Builder::basic));
  }

  static Stream<Arguments> checkProvider() {
    String instantStr = "2024-12-24T12:12:12Z";
    Instant instant = Instant.parse(instantStr);

    return Stream.of(
        arguments(
            "one",
            Opts.builder()
                .key(keySecret("key"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(tokenSecret("exp-token", instant))
                .build()),
        arguments(
            "two",
            Opts.builder()
                .key(keySecret("key"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(tokenSecret("non-exp-token", null))
                .build()));
  }

  @NessieImmutable
  public interface Opts {
    Opts EMPTY = Opts.builder().build();

    Optional<BasicCredentials> basic();

    Optional<KeySecret> key();

    Optional<TokenSecret> expiringToken();

    static Builder builder() {
      return ImmutableOpts.builder();
    }

    interface Builder {
      Builder from(Opts opts);

      Builder basic(BasicCredentials basic);

      Builder key(KeySecret key);

      Builder expiringToken(TokenSecret expiringToken);

      Opts build();
    }
  }
}
