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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.projectnessie.catalog.files.s3.CacheMetrics.statsCounter;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3StsCache;
import org.projectnessie.nessie.immutables.NessieImmutable;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

/** Maintains a pool of STS clients. */
public class StsClientsPool {
  public static final String CACHE_NAME = "sts-clients";

  private final Cache<StsClientKey, StsClient> clients;
  private final Function<StsClientKey, StsClient> clientBuilder;

  public StsClientsPool(
      S3StsCache effectiveSts, SdkHttpClient sdkHttpClient, MeterRegistry meterRegistry) {
    this(
        effectiveSts.effectiveClientsCacheMaxSize(),
        key -> defaultStsClient(key, sdkHttpClient),
        Optional.ofNullable(meterRegistry));
  }

  @VisibleForTesting
  StsClientsPool(
      int maxSize,
      Function<StsClientKey, StsClient> clientBuilder,
      Optional<MeterRegistry> meterRegistry) {
    this.clientBuilder = clientBuilder;
    this.clients =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            .recordStats(() -> statsCounter(meterRegistry, CACHE_NAME, maxSize))
            .build();
  }

  public StsClient stsClientForBucket(S3BucketOptions options) {
    String region =
        options.region().orElseThrow(() -> new IllegalArgumentException("Missing S3 region"));
    return clients.get(ImmutableStsClientKey.of(options.stsEndpoint(), region), clientBuilder);
  }

  private static StsClient defaultStsClient(StsClientKey parameters, SdkHttpClient sdkClient) {
    StsClientBuilder builder = StsClient.builder();
    builder.httpClient(sdkClient);
    if (parameters.endpoint().isPresent()) {
      CompletableFuture<Endpoint> endpointFuture =
          completedFuture(Endpoint.builder().url(parameters.endpoint().get()).build());
      builder.endpointProvider(params -> endpointFuture);
    }
    builder.region(Region.of(parameters.region()));
    return builder.build();
  }

  @NessieImmutable
  interface StsClientKey {
    Optional<URI> endpoint();

    String region();
  }
}
