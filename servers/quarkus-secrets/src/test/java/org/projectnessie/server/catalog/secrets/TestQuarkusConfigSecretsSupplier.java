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
package org.projectnessie.server.catalog.secrets;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.net.URI;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.quarkus.config.QuarkusCatalogConfig;

@ExtendWith(SoftAssertionsExtension.class)
public class TestQuarkusConfigSecretsSupplier {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void smallryeConfigSecretsSupplier() {
    Map<String, String> configs =
        Map.of(
            "foo.a", "b",
            "foo.c", "d",
            "bar.a", "b",
            "bar.c", "d");

    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(QuarkusCatalogConfig.class)
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    SmallryeConfigSecretsProvider secretsSupplier = new SmallryeConfigSecretsProvider(config);
    soft.assertThat(secretsSupplier.resolveSecret(URI.create("foo")))
        .containsExactlyInAnyOrderEntriesOf(Map.of("a", "b", "c", "d"));
    soft.assertThat(secretsSupplier.resolveSecret(URI.create("bar")))
        .containsExactlyInAnyOrderEntriesOf(Map.of("a", "b", "c", "d"));
  }

  @Test
  public void smallryeConfigSecretsProvider() {
    Map<String, String> configs =
        Map.of(
            "foo.name",
            "foo-name",
            "foo.secret",
            "foo-secret",
            "bar",
            "{\"name\":\"bar-name\", \"secret\":\"bar-secret\"}",
            "token",
            "my-token",
            "key",
            "my-secret-key");

    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(QuarkusCatalogConfig.class)
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    SecretsProvider secretsProvider = new SmallryeConfigSecretsProvider(config);

    soft.assertThat(
            secretsProvider.getSecret(URI.create("foo"), SecretType.BASIC, BasicCredentials.class))
        .get()
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("foo-name", "foo-secret");

    soft.assertThat(
            secretsProvider.getSecret(URI.create("bar"), SecretType.BASIC, BasicCredentials.class))
        .get()
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("bar-name", "bar-secret");

    soft.assertThat(
            secretsProvider.getSecret(
                URI.create("token"), SecretType.EXPIRING_TOKEN, TokenSecret.class))
        .get()
        .extracting(TokenSecret::token)
        .isEqualTo("my-token");

    soft.assertThat(secretsProvider.getSecret(URI.create("key"), SecretType.KEY, KeySecret.class))
        .get()
        .extracting(KeySecret::key)
        .isEqualTo("my-secret-key");
  }
}
