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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.catalog.files.BenchUtils.mockServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Clock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import software.amazon.awssdk.http.SdkHttpClient;

/** Microbenchmark to identify the resource footprint when using {@link S3ClientSupplier}. */
@Warmup(iterations = 3, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class S3ClientResourceBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    ObjectStorageMock.MockServer server;

    S3ClientSupplier clientSupplier;
    SdkHttpClient httpClient;

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
              .endpoint(server.getS3BaseUri())
              .pathStyleAccess(true)
              .build();

      S3Sessions sessions = new S3Sessions("foo", null);

      clientSupplier =
          new S3ClientSupplier(httpClient, s3config, s3options, secret -> "secret", sessions);
    }

    @TearDown
    public void tearDown() throws Exception {
      server.close();
      httpClient.close();
    }
  }

  @Benchmark
  public void s3client(BenchmarkParam param, Blackhole bh) {
    bh.consume(param.clientSupplier.getClient(URI.create("s3://bucket/key")));
  }

  @Benchmark
  public void s3Get(BenchmarkParam param, Blackhole bh) throws IOException {
    S3ObjectIO objectIO = new S3ObjectIO(param.clientSupplier, Clock.systemUTC());
    try (InputStream in = objectIO.readObject(URI.create("s3://bucket/key"))) {
      bh.consume(in.readAllBytes());
    }
  }

  @Benchmark
  public void s3Get250k(BenchmarkParam param, Blackhole bh) throws IOException {
    S3ObjectIO objectIO = new S3ObjectIO(param.clientSupplier, Clock.systemUTC());
    try (InputStream in = objectIO.readObject(URI.create("s3://bucket/s-256000"))) {
      bh.consume(in.readAllBytes());
    }
  }

  @Benchmark
  public void s3Get4M(BenchmarkParam param, Blackhole bh) throws IOException {
    S3ObjectIO objectIO = new S3ObjectIO(param.clientSupplier, Clock.systemUTC());
    try (InputStream in = objectIO.readObject(URI.create("s3://bucket/s-4194304"))) {
      bh.consume(in.readAllBytes());
    }
  }
}
