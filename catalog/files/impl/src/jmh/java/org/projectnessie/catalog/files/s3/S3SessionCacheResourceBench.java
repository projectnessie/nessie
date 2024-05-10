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

import java.net.URI;
import java.util.List;
import java.util.Optional;
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
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;

/** Microbenchmark to identify the resource footprint of {@link S3SessionsManager}. */
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

    S3SessionsManager s3SessionsManager;
    SdkHttpClient httpClient;

    @Param({"1", "100", "1000", "10000", "100000"})
    int numBucketOptions;

    S3BucketOptions[] bucketOptions;

    @Setup
    public void init() {
      server = mockServer(mock -> {});

      S3Config s3config = S3Config.builder().build();
      httpClient = S3Clients.apacheHttpClient(s3config);

      S3Options<S3BucketOptions> s3options =
          S3ProgrammaticOptions.builder()
              .accessKeyIdRef("foo")
              .secretAccessKeyRef("bar")
              .region("eu-central-1")
              .pathStyleAccess(true)
              .build();

      s3SessionsManager =
          new S3SessionsManager(
              s3options,
              System::currentTimeMillis,
              httpClient,
              null,
              Optional.empty(),
              secret -> "secret",
              null);

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
                      S3ProgrammaticOptions.S3PerBucketOptions.builder()
                          .accessKeyIdRef("key" + i)
                          .secretAccessKeyRef("secret" + i)
                          .externalId("externalId" + i)
                          .region(regions.get(i % regions.size()))
                          .roleSessionName("roleSessionName" + i)
                          .stsEndpoint(stsEndpoint)
                          .roleArn("roleArn" + i)
                          .build())
              .toArray(S3ProgrammaticOptions.S3PerBucketOptions[]::new);
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
    bh.consume(param.s3SessionsManager.sessionCredentialsForServer("repo", param.bucketOptions()));
  }

  @Benchmark
  public void getCredentialsForClient(BenchmarkParam param, Blackhole bh) {
    bh.consume(param.s3SessionsManager.sessionCredentialsForClient("repo", param.bucketOptions()));
  }
}
