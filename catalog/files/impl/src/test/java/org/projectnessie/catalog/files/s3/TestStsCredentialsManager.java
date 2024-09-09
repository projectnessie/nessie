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
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.ImmutableS3ClientIam;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3ServerIam;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3ClientIam;
import org.projectnessie.catalog.files.config.S3ServerIam;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.services.sts.model.Credentials;

@ExtendWith(SoftAssertionsExtension.class)
class TestStsCredentialsManager {

  @InjectSoftAssertions protected SoftAssertions soft;

  private static Credentials credentials(long expiryTimeMillis) {
    return Credentials.builder().expiration(Instant.ofEpochMilli(expiryTimeMillis)).build();
  }

  @Test
  void testExpiration() {
    AtomicLong time = new AtomicLong();

    MockStsCredentialsFetcher loader = new MockStsCredentialsFetcher();

    StsCredentialsManager manager =
        new StsCredentialsManager(10, Duration.ofMillis(10), loader, time::get, Optional.empty());
    S3BucketOptions options =
        ImmutableS3NamedBucketOptions.builder()
            .region("R1")
            .serverIam(ImmutableS3ServerIam.builder().enabled(true).assumeRole("role").build())
            .build();

    loader.credentials.set(credentials(time.get() + 100));
    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(1);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(1);

    time.set(89); // just before the expiry time minus grace time
    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(1);

    time.set(90); // at the grace period - the entry is expired
    loader.credentials.set(credentials(time.get() + 200));

    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(2);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(2);

    // test expiry in the past
    time.set(1000);
    loader.credentials.set(
        Credentials.builder().expiration(Instant.ofEpochMilli(time.get() - 1)).build());
    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(3);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(4);
  }

  @Test
  void testClientSessionCredentials() {
    AtomicLong time = new AtomicLong();

    MockStsCredentialsFetcher loader = new MockStsCredentialsFetcher();

    StsCredentialsManager manager =
        new StsCredentialsManager(10, Duration.ofMillis(10), loader, time::get, Optional.empty());
    S3BucketOptions options =
        ImmutableS3NamedBucketOptions.builder()
            .region("R1")
            .clientIam(ImmutableS3ClientIam.builder().enabled(true).assumeRole("role").build())
            .build();

    loader.credentials.set(credentials(time.get() + 100));

    StorageUri location = StorageUri.of("s3://bucket/path");
    StorageLocations locations =
        StorageLocations.storageLocations(
            StorageUri.of("s3://bucket/"), List.of(location), List.of());
    soft.assertThat(manager.sessionCredentialsForClient(options, locations))
        .isSameAs(loader.credentials.get());
    soft.assertThat(loader.counter.get()).isEqualTo(1);

    soft.assertThat(manager.sessionCredentialsForClient(options, locations))
        .isSameAs(loader.credentials.get());
    // Client session credentials are not cached
    soft.assertThat(loader.counter.get()).isEqualTo(2);
  }

  @Test
  void testRepositoryIsolation() {
    AtomicLong time = new AtomicLong();

    MockStsCredentialsFetcher loader = new MockStsCredentialsFetcher();

    StsCredentialsManager manager =
        new StsCredentialsManager(10, Duration.ofMillis(10), loader, time::get, Optional.empty());

    S3BucketOptions options =
        ImmutableS3NamedBucketOptions.builder()
            .region("R1")
            .serverIam(ImmutableS3ServerIam.builder().enabled(true).assumeRole("role").build())
            .build();
    Credentials c1 = credentials(time.get() + 100);
    Credentials c2 = credentials(time.get() + 200);

    loader.credentials.set(c1);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(c1);
    loader.credentials.set(c2);
    soft.assertThat(manager.sessionCredentialsForServer("r2", options)).isSameAs(c2);

    // cached responses
    loader.credentials.set(null);
    soft.assertThat(manager.sessionCredentialsForServer("r1", options)).isSameAs(c1);
    soft.assertThat(manager.sessionCredentialsForServer("r2", options)).isSameAs(c2);
  }

  @Test
  void testMetrics() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    new StsCredentialsManager(
        10,
        Duration.ofMillis(10),
        new MockStsCredentialsFetcher(),
        () -> 1L,
        Optional.of(meterRegistry));

    Function<Meter, AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>>> extractor =
        meter ->
            assertThat(meter)
                .extracting(
                    m -> m.getId().getTag("cache"),
                    m -> m.getId().getName(),
                    m -> m.measure().iterator().next().getValue());

    soft.assertThat(meterRegistry.getMeters())
        .describedAs(meterRegistry.getMetersAsString())
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-sessions", "cache.loads", 0.0d))
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-sessions", "max_entries", 10.0d));
  }

  static class MockStsCredentialsFetcher implements StsCredentialsFetcher {

    final AtomicReference<Credentials> credentials;
    final AtomicInteger counter;

    MockStsCredentialsFetcher() {
      this.credentials = new AtomicReference<>();
      this.counter = new AtomicInteger();
    }

    @Override
    public Credentials fetchCredentialsForClient(
        S3BucketOptions bucketOptions, S3ClientIam iam, Optional<StorageLocations> locations) {
      counter.incrementAndGet();
      return credentials.get();
    }

    @Override
    public Credentials fetchCredentialsForServer(S3BucketOptions bucketOptions, S3ServerIam iam) {
      counter.incrementAndGet();
      return credentials.get();
    }
  }
}
