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
package org.projectnessie.catalog.secrets.smallrye;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsManager;
import org.projectnessie.catalog.secrets.TokenSecret;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSmallryeConfigSecretsProvider {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void resolveSecrets() {
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
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    SmallryeConfigSecretsManager secretsSupplier = new SmallryeConfigSecretsManager(config);
    soft.assertThat(secretsSupplier.resolveSecret("foo"))
        .containsExactlyInAnyOrderEntriesOf(Map.of("a", "b", "c", "d"));
    soft.assertThat(secretsSupplier.resolveSecret("bar"))
        .containsExactlyInAnyOrderEntriesOf(Map.of("a", "b", "c", "d"));
  }

  @Test
  public void getSecrets() {
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
            .withSources(new PropertiesConfigSource(configs, "configSource", 100))
            .build();

    SecretsManager secretsManager = new SmallryeConfigSecretsManager(config);

    soft.assertThat(secretsManager.getSecret("foo", SecretType.BASIC, BasicCredentials.class))
        .get()
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("foo-name", "foo-secret");

    soft.assertThat(secretsManager.getSecret("bar", SecretType.BASIC, BasicCredentials.class))
        .get()
        .extracting(BasicCredentials::name, BasicCredentials::secret)
        .containsExactly("bar-name", "bar-secret");

    soft.assertThat(secretsManager.getSecret("token", SecretType.EXPIRING_TOKEN, TokenSecret.class))
        .get()
        .extracting(TokenSecret::token)
        .isEqualTo("my-token");

    soft.assertThat(secretsManager.getSecret("key", SecretType.KEY, KeySecret.class))
        .get()
        .extracting(KeySecret::key)
        .isEqualTo("my-secret-key");
  }
}
