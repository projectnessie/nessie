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

import static java.lang.Math.floorMod;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.catalog.files.BenchUtils.mockServer;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.ImmutableS3ClientIam;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.ImmutableS3ServerIam;
import org.projectnessie.catalog.files.config.ImmutableS3StsCache;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.config.S3StsCache;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;

/** Microbenchmark to identify the resource footprint of {@link StsCredentialsManager}. */
@Warmup(iterations = 3, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class S3SessionCacheResourceBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    ObjectStorageMock.MockServer server;

    StsCredentialsManager stsCredentialsManager;
    SdkHttpClient httpClient;

    @Param({"1", "100", "1000", "10000", "100000"})
    int numBucketOptions;

    S3BucketOptions[] bucketOptions;

    @Setup
    public void init() {
      server = mockServer(mock -> {});

      Map<String, Map<String, String>> secretsMap = new HashMap<>();
      for (int i = 0; i < numBucketOptions; i++) {
        secretsMap.put("access-key-" + i, basicCredentials("foo" + i, "bar" + 1).asMap());
      }
      secretsMap.put("the-access-key", basicCredentials("foo", "bar").asMap());
      SecretsProvider secretsProvider =
          ResolvingSecretsProvider.builder()
              .putSecretsManager("plain", unsafePlainTextSecretsProvider(secretsMap))
              .build();

      S3Config s3config = S3Config.builder().build();
      httpClient = S3Clients.apacheHttpClient(s3config, secretsProvider);

      S3Options s3options =
          ImmutableS3Options.builder()
              .defaultOptions(
                  ImmutableS3NamedBucketOptions.builder()
                      .accessKey(URI.create("the-access-key"))
                      .region("eu-central-1")
                      .pathStyleAccess(true)
                      .build())
              .build();

      S3StsCache sts = ImmutableS3StsCache.builder().build();
      StsClientsPool stsClientsPool = new StsClientsPool(sts, httpClient, null);
      stsCredentialsManager = new StsCredentialsManager(sts, stsClientsPool, secretsProvider, null);

      List<String> regions =
          Region.regions().stream()
              .filter(r -> !r.isGlobalRegion())
              .map(Region::id)
              .collect(Collectors.toList());

      URI stsEndpoint = server.getStsEndpointURI();
      bucketOptions =
          IntStream.range(0, numBucketOptions)
              .mapToObj(
                  i ->
                      ImmutableS3NamedBucketOptions.builder()
                          .accessKey(URI.create("access-key-" + 1))
                          .region(regions.get(i % regions.size()))
                          .stsEndpoint(stsEndpoint)
                          .clientIam(
                              ImmutableS3ClientIam.builder()
                                  .externalId("externalId" + i)
                                  .roleSessionName("roleSessionName" + i)
                                  .assumeRole("roleArn" + i)
                                  .enabled(true)
                                  .build())
                          .serverIam(
                              ImmutableS3ServerIam.builder()
                                  .externalId("externalId" + i)
                                  .roleSessionName("roleSessionName" + i)
                                  .assumeRole("roleArn" + i)
                                  .enabled(true)
                                  .build())
                          .build())
              .toArray(ImmutableS3NamedBucketOptions[]::new);
    }

    @TearDown
    public void tearDown() throws Exception {
      server.close();
      httpClient.close();
    }

    S3BucketOptions bucketOptions() {
      return bucketOptions[floorMod(ThreadLocalRandom.current().nextInt(), bucketOptions.length)];
    }
  }

  @Benchmark
  public void getCredentialsForServer(BenchmarkParam param, Blackhole bh) {
    bh.consume(
        param.stsCredentialsManager.sessionCredentialsForServer("repo", param.bucketOptions()));
  }

  @Benchmark
  public void getCredentialsForClient(BenchmarkParam param, Blackhole bh) {
    bh.consume(
        param.stsCredentialsManager.sessionCredentialsForClient(
            param.bucketOptions(),
            StorageLocations.storageLocations(
                StorageUri.of("s3://bucket/"),
                List.of(StorageUri.of("s3://bucket/path")),
                List.of())));
  }
}
