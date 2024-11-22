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

import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.SecretType.BASIC;
import static org.projectnessie.catalog.secrets.SecretType.KEY;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsManager;
import org.projectnessie.catalog.secrets.SecretsProvider;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCachingSecretsBackend {
  public static final String URN_SECRET_PREFIX = "urn:nessie-secret:mine:";
  @InjectSoftAssertions protected SoftAssertions soft;

  static final Secret SECRET_1_REPO_1 = keySecret("secret1-repo1");
  static final Secret SECRET_2_REPO_1 = basicCredentials("secret1-name", "secret1-value");
  static final Secret SECRET_1_REPO_2 = keySecret("secret1-repo2");
  static final Secret SECRET_2_REPO_2 = basicCredentials("secret2-name", "secret2-value");

  AtomicLong clock;
  CachingSecretsBackend backend;
  CachingSecrets cachingSecrets;
  SecretsManager secrets1;
  SecretsManager secrets2;
  SecretsProvider provider1;
  SecretsProvider provider2;
  SecretsProvider caching1;
  SecretsProvider caching2;
  URI secret1;
  URI secret2;

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

    String secret1 = "secret1";
    String secret2 = "secret2";
    this.secret1 = URI.create(URN_SECRET_PREFIX + "secret1");
    this.secret2 = URI.create(URN_SECRET_PREFIX + "secret2");

    secrets1 =
        unsafePlainTextSecretsProvider(
            Map.of(secret1, SECRET_1_REPO_1.asMap(), secret2, SECRET_2_REPO_1.asMap()));
    secrets2 =
        unsafePlainTextSecretsProvider(
            Map.of(secret1, SECRET_1_REPO_2.asMap(), secret2, SECRET_2_REPO_2.asMap()));

    provider1 = ResolvingSecretsProvider.builder().putSecretsManager("mine", secrets1).build();
    provider2 = ResolvingSecretsProvider.builder().putSecretsManager("mine", secrets2).build();

    caching1 = cachingSecrets.forRepository("repo1", provider1);
    caching2 = cachingSecrets.forRepository("repo2", provider2);
  }

  @Test
  public void ttlExpire() {

    soft.assertThat(caching1.getSecret(secret1, KEY, KeySecret.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_1_REPO_1.asMap());
    soft.assertThat(caching2.getSecret(secret1, KEY, KeySecret.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_1_REPO_2.asMap());
    soft.assertThat(caching1.getSecret(secret2, BASIC, BasicCredentials.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_2_REPO_1.asMap());
    soft.assertThat(caching2.getSecret(secret2, BASIC, BasicCredentials.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_2_REPO_2.asMap());

    soft.assertThat(backend.cache.asMap())
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo1", secret1, 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo2", secret1, 0));

    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));

    soft.assertThat(backend.cache.asMap())
        .doesNotContainKey(new CachingSecretsBackend.CacheKeyValue("repo1", secret1, 0))
        .doesNotContainKey(new CachingSecretsBackend.CacheKeyValue("repo2", secret1, 0));

    soft.assertThat(caching1.getSecret(secret1, KEY, KeySecret.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_1_REPO_1.asMap());
    soft.assertThat(caching2.getSecret(secret1, KEY, KeySecret.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_1_REPO_2.asMap());
    soft.assertThat(caching1.getSecret(secret2, BASIC, BasicCredentials.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_2_REPO_1.asMap());
    soft.assertThat(caching2.getSecret(secret2, BASIC, BasicCredentials.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_2_REPO_2.asMap());

    soft.assertThat(backend.cache.asMap())
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo1", secret1, 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo2", secret1, 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo1", secret2, 0))
        .containsKey(new CachingSecretsBackend.CacheKeyValue("repo2", secret2, 0));
  }

  @Test
  public void nonExisting() {
    URI secretNope1 = URI.create(URN_SECRET_PREFIX + "secret-nope1");
    URI secretNope2 = URI.create(URN_SECRET_PREFIX + "secret-nope2");
    soft.assertThat(caching1.getSecret(secretNope1, KEY, KeySecret.class)).isEmpty();
    soft.assertThat(caching1.getSecret(secretNope2, KEY, KeySecret.class)).isEmpty();
    soft.assertThat(caching2.getSecret(secretNope1, KEY, KeySecret.class)).isEmpty();
    soft.assertThat(caching2.getSecret(secretNope2, KEY, KeySecret.class)).isEmpty();
    soft.assertThat(caching1.getSecret(secret1, KEY, KeySecret.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_1_REPO_1.asMap());
    soft.assertThat(caching2.getSecret(secret1, KEY, KeySecret.class))
        .get()
        .extracting(Secret::asMap, map(String.class, String.class))
        .containsExactlyInAnyOrderEntriesOf(SECRET_1_REPO_2.asMap());
  }

  @Test
  public void tooManyEntries() {
    SecretsProvider supplier =
        new SecretsProvider() {
          @Override
          public <S extends Secret> Optional<S> getSecret(
              @Nonnull URI name, @Nonnull SecretType secretType, @Nonnull Class<S> secretJavaType) {
            return Optional.of(secretJavaType.cast(keySecret(name.toString())));
          }
        };
    SecretsProvider caching = cachingSecrets.forRepository("repoMany", supplier);

    // Very naive test that checks that expiration happens - our "weigher" does something.

    for (int i = 0; i < 100_000; i++) {
      caching.getSecret(
          URI.create(
              "wepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoewwepofkjeopwkfopwekfopkewfkpoew-"
                  + i),
          KEY,
          KeySecret.class);
      // In fact, the cache should never have more than 10000 entries, but 'estimatedSize' is really
      // just an estimate.
      soft.assertThat(backend.cache.estimatedSize()).isLessThan(10_000);
    }
  }
}
