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

import static org.projectnessie.catalog.files.s3.CacheMetrics.statsCounter;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.checkerframework.checker.index.qual.NonNegative;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3ClientIam;
import org.projectnessie.catalog.files.config.S3ServerIam;
import org.projectnessie.catalog.files.config.S3StsCache;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.nessie.immutables.NessieImmutable;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Manages refreshing STS session credentials on demand. */
public class StsCredentialsManager {
  public static final String CACHE_NAME = "sts-sessions";

  private final LoadingCache<SessionKey, Credentials> sessions;
  private final Duration expiryReduction;
  private final StsCredentialsFetcher credentialsFetcher;

  public StsCredentialsManager(
      S3StsCache effectiveSts,
      StsClientsPool clients,
      SecretsProvider secretsProvider,
      MeterRegistry meterRegistry) {
    this(
        effectiveSts.effectiveSessionCacheMaxSize(),
        effectiveSts.effectiveSessionGracePeriod(),
        new StsCredentialsFetcherImpl(clients, secretsProvider),
        System::currentTimeMillis,
        Optional.ofNullable(meterRegistry));
  }

  @VisibleForTesting
  StsCredentialsManager(
      int maxSize,
      Duration expiryReduction,
      StsCredentialsFetcher credentialsFetcher,
      LongSupplier systemTimeMillis,
      Optional<MeterRegistry> meterRegistry) {
    this.credentialsFetcher = credentialsFetcher;
    this.expiryReduction = expiryReduction;
    this.sessions =
        Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .ticker(() -> TimeUnit.MILLISECONDS.toNanos(systemTimeMillis.getAsLong()))
            .maximumSize(maxSize)
            .recordStats(() -> statsCounter(meterRegistry, CACHE_NAME, maxSize))
            .expireAfter(new StsSessionsExpiry())
            .build(key -> loadServerSessionCredentials(key.bucketOptions()));
  }

  private Credentials loadServerSessionCredentials(S3BucketOptions options) {
    S3ServerIam iam =
        options
            .getEnabledServerIam()
            .orElseThrow(() -> new IllegalStateException("server IAM not enabled"));
    return credentialsFetcher.fetchCredentialsForServer(options, iam);
  }

  /**
   * Returns new STS session credentials suitable for client credentials vending on the given S3
   * bucket. These credentials are not cached so each call to this method returns new credentials.
   */
  public Credentials sessionCredentialsForClient(
      S3BucketOptions options, StorageLocations locations) {
    S3ClientIam iam =
        options
            .getEnabledClientIam()
            .orElseThrow(() -> new IllegalStateException("client IAM not enabled"));
    return credentialsFetcher.fetchCredentialsForClient(
        options, iam, Optional.ofNullable(locations));
  }

  /**
   * Returns STS session credentials suitable for the catalog server itself and the given S3 bucket.
   * These credentials are cached.
   */
  public Credentials sessionCredentialsForServer(String repositoryId, S3BucketOptions options) {
    ImmutableSessionKey sessionKey =
        ImmutableSessionKey.builder().repositoryId(repositoryId).bucketOptions(options).build();
    return sessions.get(sessionKey);
  }

  @NessieImmutable
  interface SessionKey {
    String repositoryId();

    S3BucketOptions bucketOptions();
  }

  private class StsSessionsExpiry implements Expiry<SessionKey, Credentials> {

    @Override
    public long expireAfterCreate(
        @Nonnull SessionKey key, @Nonnull Credentials value, long currentTimeNanos) {
      Instant expiration = value.expiration();
      long currentTimeMillis = TimeUnit.NANOSECONDS.toMillis(currentTimeNanos);
      currentTimeMillis += expiryReduction.toMillis();
      Instant effectiveNow = Instant.ofEpochMilli(currentTimeMillis);
      long lifetimeMillis =
          expiration.isBefore(effectiveNow) ? 0 : effectiveNow.until(expiration, ChronoUnit.MILLIS);
      return TimeUnit.MILLISECONDS.toNanos(lifetimeMillis);
    }

    @Override
    public long expireAfterUpdate(
        @Nonnull SessionKey key,
        @Nonnull Credentials value,
        long currentTime,
        @NonNegative long currentDuration) {
      return currentDuration;
    }

    @Override
    public long expireAfterRead(
        @Nonnull SessionKey key,
        @Nonnull Credentials value,
        long currentTime,
        @NonNegative long currentDuration) {
      return currentDuration;
    }
  }
}
