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
package org.projectnessie.catalog.secrets.cache;

import static java.util.function.Function.identity;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.spi.SecretsSupplier;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCachingSecretsBackend {
  @InjectSoftAssertions protected SoftAssertions soft;

  static final Secret SECRET_1_REPO_1 = keySecret("secret1-repo1");
  static final Secret SECRET_2_REPO_1 = basicCredentials("secret1-name", "secret1-value");
  static final Secret SECRET_1_REPO_2 = keySecret("secret1-repo2");
  static final Secret SECRET_2_REPO_2 = basicCredentials("secret2-name", "secret2-value");
  static final Map<String, SecretType> SECRETS_1 = Map.of("secret1", SecretType.KEY);
  static final Map<String, SecretType> SECRETS_1_2 =
      Map.of("secret1", SecretType.KEY, "secret2", SecretType.KEY);

  AtomicLong clock;
  CachingSecretsBackend backend;
  CachingSecrets cachingSecrets;
  SecretsSupplier secrets1;
  SecretsSupplier secrets2;
  SecretsSupplier caching1;
  SecretsSupplier caching2;

  @BeforeEach
  void setup() {
    clock = new AtomicLong();
    backend =
        new CachingSecretsBackend(
            SecretsCacheConfig.builder()
                .maxElements(100)
                .ttlMillis(1000)
                .clockNanos(clock::get)
                .build());

    cachingSecrets = new CachingSecrets(backend);

    secrets1 = mapSecretsSupplier(Map.of("secret1", SECRET_1_REPO_1, "secret2", SECRET_2_REPO_1));
    secrets2 = mapSecretsSupplier(Map.of("secret1", SECRET_1_REPO_2, "secret2", SECRET_2_REPO_2));

    caching1 = cachingSecrets.forRepository("repo1", secrets1);
    caching2 = cachingSecrets.forRepository("repo2", secrets2);
  }

  @Test
  public void ttlExpire() {
    soft.assertThat(caching1.resolveSecrets(SECRETS_1))
        .containsExactlyInAnyOrderEntriesOf(Map.of("secret1", SECRET_1_REPO_1));
    soft.assertThat(caching2.resolveSecrets(SECRETS_1))
        .containsExactlyInAnyOrderEntriesOf(Map.of("secret1", SECRET_1_REPO_2));
    soft.assertThat(caching1.resolveSecrets(SECRETS_1))
        .containsExactlyInAnyOrderEntriesOf(Map.of("secret1", SECRET_1_REPO_1));
    soft.assertThat(caching2.resolveSecrets(SECRETS_1))
        .containsExactlyInAnyOrderEntriesOf(Map.of("secret1", SECRET_1_REPO_2));

    soft.assertThat(backend.cache.asMap())
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo1", "secret1", 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo2", "secret1", 0));

    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));

    soft.assertThat(backend.cache.asMap())
        .doesNotContainKey(new CachingSecretsBackend.CacheKeyValue("repo1", "secret1", 0))
        .doesNotContainKey(new CachingSecretsBackend.CacheKeyValue("repo2", "secret1", 0));

    soft.assertThat(caching1.resolveSecrets(SECRETS_1_2))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("secret1", SECRET_1_REPO_1, "secret2", SECRET_2_REPO_1));
    soft.assertThat(caching2.resolveSecrets(SECRETS_1_2))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("secret1", SECRET_1_REPO_2, "secret2", SECRET_2_REPO_2));

    soft.assertThat(backend.cache.asMap())
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo1", "secret1", 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo2", "secret1", 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo1", "secret2", 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo2", "secret2", 0));
  }

  @Test
  public void nonExisting() {
    soft.assertThat(
            caching1.resolveSecrets(
                Map.of("secret-nope1", SecretType.KEY, "secret-nope2", SecretType.KEY)))
        .isEmpty();
    soft.assertThat(
            caching2.resolveSecrets(
                Map.of("secret-nope1", SecretType.KEY, "secret-nope2", SecretType.KEY)))
        .isEmpty();
    soft.assertThat(
            caching1.resolveSecrets(
                Map.of(
                    "secret-nope1",
                    SecretType.KEY,
                    "secret-nope2",
                    SecretType.KEY,
                    "secret1",
                    SecretType.KEY)))
        .containsExactlyInAnyOrderEntriesOf(Map.of("secret1", SECRET_1_REPO_1));
    soft.assertThat(
            caching2.resolveSecrets(
                Map.of(
                    "secret-nope1",
                    SecretType.KEY,
                    "secret-nope2",
                    SecretType.KEY,
                    "secret1",
                    SecretType.KEY)))
        .containsExactlyInAnyOrderEntriesOf(Map.of("secret1", SECRET_1_REPO_2));
  }

  @Test
  public void tooManyEntries() {
    SecretsSupplier supplier =
        toResolve ->
            toResolve.keySet().stream()
                .collect(Collectors.toMap(identity(), name -> keySecret(name)));
    SecretsSupplier caching = cachingSecrets.forRepository("repoMany", supplier);

    // Very naive test that checks that expiration happens - our "weigher" does something.

    for (int i = 0; i < 100_000; i++) {
      caching.resolveSecrets(
          Map.of(
              "wepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoew-"
                  + i,
              SecretType.KEY));
      // In fact, the cache should never have more than 10000 entries, but 'estimatedSize' is really
      // just an estimate.
      soft.assertThat(backend.cache.estimatedSize()).isLessThan(10_000);
    }
  }

  static SecretsSupplier mapSecretsSupplier(Map<String, Secret> secretsMap) {
    return toResolve -> {
      Map<String, Secret> resolved = new HashMap<>();
      for (String name : toResolve.keySet()) {
        Secret r = secretsMap.get(name);
        if (r != null) {
          resolved.put(name, r);
        }
      }
      return resolved;
    };
  }
}
