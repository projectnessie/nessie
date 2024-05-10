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
package org.projectnessie.catalog.files.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.projectnessie.catalog.files.s3.S3SessionsManager.SessionCredentialsFetcher;
import org.projectnessie.catalog.files.s3.S3SessionsManager.StsClientKey;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

@ExtendWith(SoftAssertionsExtension.class)
class TestS3SessionsManager {

  @InjectSoftAssertions protected SoftAssertions soft;

  private static final S3Options<?> s3options =
      S3ProgrammaticOptions.builder()
          .sessionCredentialCacheMaxEntries(10)
          .stsClientsCacheMaxEntries(2)
          .sessionCredentialRefreshGracePeriod(Duration.ofMillis(10))
          .build();

  private static Credentials credentials(long expiryTimeMillis) {
    return Credentials.builder().expiration(Instant.ofEpochMilli(expiryTimeMillis)).build();
  }

  @Test
  void testExpiration() {
    AtomicLong time = new AtomicLong();

    AtomicInteger clientCounter = new AtomicInteger();
    Function<StsClientKey, StsClient> clientBuilder =
        (parameters) -> {
          clientCounter.incrementAndGet();
          return Mockito.mock(StsClient.class);
        };

    AtomicInteger counter = new AtomicInteger();
    AtomicReference<Credentials> credentials = new AtomicReference<>();
    SessionCredentialsFetcher loader =
        (client, key, duration) -> {
          counter.incrementAndGet();
          return credentials.get();
        };

    S3SessionsManager manager =
        new S3SessionsManager(
            s3options, time::get, null, clientBuilder, Optional.empty(), s -> s, loader);
    S3BucketOptions options = S3ProgrammaticOptions.builder().region("R1").roleArn("role").build();

    credentials.set(credentials(time.get() + 100));
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(1);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(1);

    time.set(89); // just before the expiry time minus grace time
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(counter.get()).isEqualTo(1);

    time.set(90); // at the grace period - the entry is expired
    credentials.set(credentials(time.get() + 200));

    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(2);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(2);

    // test expiry in the past
    time.set(1000);
    credentials.set(Credentials.builder().expiration(Instant.ofEpochMilli(time.get() - 1)).build());
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(3);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(4);
  }

  @Test
  void testClientSessionCredentials() {
    AtomicLong time = new AtomicLong();

    AtomicInteger clientCounter = new AtomicInteger();
    Function<StsClientKey, StsClient> clientBuilder =
        (parameters) -> {
          clientCounter.incrementAndGet();
          return Mockito.mock(StsClient.class);
        };

    AtomicInteger counter = new AtomicInteger();
    AtomicReference<Credentials> credentials = new AtomicReference<>();
    SessionCredentialsFetcher loader =
        (client, key, duration) -> {
          counter.incrementAndGet();
          return credentials.get();
        };

    S3SessionsManager manager =
        new S3SessionsManager(
            s3options, time::get, null, clientBuilder, Optional.empty(), s -> s, loader);
    S3BucketOptions options = S3ProgrammaticOptions.builder().region("R1").roleArn("role").build();

    credentials.set(credentials(time.get() + 100));

    soft.assertThat(manager.sessionCredentialsForClient("r1", options)).isSameAs(credentials.get());
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    soft.assertThat(counter.get()).isEqualTo(1);

    soft.assertThat(manager.sessionCredentialsForClient("r1", options)).isSameAs(credentials.get());
    // STS clients are cached
    soft.assertThat(clientCounter.get()).isEqualTo(1);
    // Client session credentials are not cached
    soft.assertThat(counter.get()).isEqualTo(2);
  }

  @Test
  void testRepositoryIsolation() {
    AtomicLong time = new AtomicLong();

    AtomicReference<Credentials> credentials = new AtomicReference<>();
    SessionCredentialsFetcher loader = (client, key, duration) -> credentials.get();
    S3SessionsManager manager =
        new S3SessionsManager(
            s3options, time::get, null, (p) -> null, Optional.empty(), s -> s, loader);

    S3BucketOptions options = S3ProgrammaticOptions.builder().region("R1").roleArn("role").build();
    Credentials c1 = credentials(time.get() + 100);
    Credentials c2 = credentials(time.get() + 200);

    credentials.set(c1);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(c1);
    credentials.set(c2);
    soft.assertThat(manager.sessionCredentialsForServer("r2", options)).isSameAs(c2);

    // cached responses
    credentials.set(null);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(c1);
    soft.assertThat(manager.sessionCredentialsForServer("r2", options)).isSameAs(c2);
  }

  @Test
  void testMetrics() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    new S3SessionsManager(
        s3options,
        () -> 1L,
        null,
        (p) -> null,
        Optional.of(meterRegistry),
        s -> s,
        (client, key, duration) -> null);

    Function<Meter, AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>>> extractor =
        meter ->
            assertThat(meter)
                .extracting(
                    m -> m.getId().getTag("cache"),
                    m -> m.getId().getName(),
                    m -> m.measure().iterator().next().getValue());

    soft.assertThat(meterRegistry.getMeters())
        .describedAs(meterRegistry.getMetersAsString())
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-clients", "cache.loads", 0.0d))
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-clients", "max_entries", 2.0d))
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-sessions", "cache.loads", 0.0d))
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-sessions", "max_entries", 10.0d));
  }
}
