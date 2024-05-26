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

import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.ExpiringTokenSecret.expiringTokenSecret;
import static org.projectnessie.catalog.secrets.SecretAttribute.secretAttribute;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.secrets.spi.SecretsSupplier;
import org.projectnessie.nessie.immutables.NessieImmutable;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSecretsProvider {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void secretsHelper(
      Opts base, Opts spec, Opts expected, Map<String, Map<String, String>> secrets) {
    List<SecretAttribute<Opts, Opts.Builder, ?>> attributes =
        List.of(
            secretAttribute("key", SecretType.KEY, Opts::key, Opts.Builder::key),
            secretAttribute("token", SecretType.TOKEN, Opts::token, Opts.Builder::token),
            secretAttribute(
                "expiringToken",
                SecretType.EXPIRING_TOKEN,
                Opts::expiringToken,
                Opts.Builder::expiringToken),
            secretAttribute("basic", SecretType.BASIC, Opts::basic, Opts.Builder::basic));

    SecretsProvider provider = new SecretsProvider(mapSecretsSupplier(secrets));

    Opts.Builder builder = Opts.builder().from(base);
    if (spec != null) {
      builder.from(spec);
    }

    Opts opts = provider.applySecrets(builder, "base", base, "spec", spec, attributes).build();

    soft.assertThat(opts.key().map(KeySecret::key)).isEqualTo(expected.key().map(KeySecret::key));
    soft.assertThat(opts.token().map(TokenSecret::token))
        .isEqualTo(expected.token().map(TokenSecret::token));
    soft.assertThat(opts.basic().map(b -> tuple(b.name(), b.secret())))
        .isEqualTo(expected.basic().map(b -> tuple(b.name(), b.secret())));
    soft.assertThat(opts.expiringToken().map(e -> tuple(e.token(), e.expiresAt())))
        .isEqualTo(expected.expiringToken().map(e -> tuple(e.token(), e.expiresAt())));
  }

  public static Stream<Arguments> secretsHelper() {
    String instantStr = "2024-12-24T12:12:12Z";
    Instant instant = Instant.parse(instantStr);
    return Stream.of(
        arguments(Opts.EMPTY, null, Opts.EMPTY, Map.of()),
        arguments(Opts.EMPTY, Opts.EMPTY, Opts.EMPTY, Map.of()),
        //
        // invalid "name" + "token" fields
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            Opts.EMPTY,
            Map.of(
                "base.spec.basic",
                Map.of("namex", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("tokenx", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        // invalid "expiresAt" + "secret"
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            Opts.builder().expiringToken(expiringTokenSecret("exp-token", null)).build(),
            Map.of(
                "base.spec.basic",
                Map.of("name", "basic-name", "secretx", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "ewpfoijkewoipfjewijfo"))),
        // invalid "expiresAt"
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            Opts.builder().expiringToken(expiringTokenSecret("exp-token", null)).build(),
            Map.of(
                "base.spec.basic",
                Map.of("name", "basic-name", "secretx", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "42"))),
        // all secrets via "spec" from secrets-supplier
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.key",
                Map.of("value", "key"),
                "base.token",
                Map.of("value", "token"),
                "base.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        // key present
        arguments(
            Opts.EMPTY,
            Opts.builder().key(KeySecret.keySecret("key")).build(),
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        arguments(
            Opts.builder().key(KeySecret.keySecret("key")).build(),
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        // token present
        arguments(
            Opts.EMPTY,
            Opts.builder().token(TokenSecret.tokenSecret("token")).build(),
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        arguments(
            Opts.builder().token(TokenSecret.tokenSecret("token")).build(),
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        // basic present
        arguments(
            Opts.builder().basic(basicCredentials("basic-name", "basic-secret")).build(),
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        arguments(
            Opts.EMPTY,
            Opts.builder().basic(basicCredentials("basic-name", "basic-secret")).build(),
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        // expiringToken present
        arguments(
            Opts.builder().expiringToken(expiringTokenSecret("exp-token", instant)).build(),
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"))),
        arguments(
            Opts.EMPTY,
            Opts.builder().expiringToken(expiringTokenSecret("exp-token", instant)).build(),
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"))),
        // override in "base"
        arguments(
            Opts.builder()
                .key(KeySecret.keySecret("nope"))
                .token(TokenSecret.tokenSecret("nope"))
                .basic(basicCredentials("nope", "basic-nope"))
                .expiringToken(expiringTokenSecret("nope", instant))
                .build(),
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("key"))
                .token(TokenSecret.tokenSecret("token"))
                .basic(basicCredentials("basic-name", "basic-secret"))
                .expiringToken(expiringTokenSecret("exp-token", instant))
                .build(),
            Map.of(
                "base.spec.key",
                Map.of("value", "key"),
                "base.spec.token",
                Map.of("value", "token"),
                "base.spec.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.spec.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        // NO override in "base"
        arguments(
            Opts.builder()
                .key(KeySecret.keySecret("nope"))
                .token(TokenSecret.tokenSecret("nope"))
                .basic(basicCredentials("nope", "basic-nope"))
                .expiringToken(expiringTokenSecret("nope", instant))
                .build(),
            Opts.EMPTY,
            Opts.builder()
                .key(KeySecret.keySecret("nope"))
                .token(TokenSecret.tokenSecret("nope"))
                .basic(basicCredentials("nope", "basic-nope"))
                .expiringToken(expiringTokenSecret("nope", instant))
                .build(),
            Map.of(
                "base.key",
                Map.of("value", "key"),
                "base.token",
                Map.of("value", "token"),
                "base.basic",
                Map.of("name", "basic-name", "secret", "basic-secret"),
                "base.expiringToken",
                Map.of("token", "exp-token", "expiresAt", "2024-12-24T12:12:12Z"))),
        //
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            Opts.builder().expiringToken(expiringTokenSecret("exp-token", null)).build(),
            Map.of("base.spec.expiringToken", Map.of("token", "exp-token")))
        //
        );
  }

  @NessieImmutable
  public interface Opts {
    Opts EMPTY = Opts.builder().build();

    Optional<BasicCredentials> basic();

    Optional<KeySecret> key();

    Optional<TokenSecret> token();

    Optional<ExpiringTokenSecret> expiringToken();

    static Builder builder() {
      return ImmutableOpts.builder();
    }

    interface Builder {
      Builder from(Opts opts);

      Builder basic(BasicCredentials basic);

      Builder key(KeySecret key);

      Builder token(TokenSecret token);

      Builder expiringToken(ExpiringTokenSecret expiringToken);

      Opts build();
    }
  }

  static SecretsSupplier mapSecretsSupplier(Map<String, Map<String, String>> secretsMap) {
    return names -> {
      Map<String, Map<String, String>> resolved = new HashMap<>();
      for (String name : names) {
        Map<String, String> r = secretsMap.get(name);
        if (r != null) {
          resolved.put(name, r);
        }
      }
      return resolved;
    };
  }
}
