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
package org.projectnessie.catalog.files.gcs;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.catalog.files.BenchUtils.mockServer;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import com.google.auth.http.HttpTransportFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
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
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsConfig;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsConfig;
import org.projectnessie.catalog.files.config.ImmutableGcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;

/** Microbenchmark to identify the resource footprint when using {@link GcsStorageSupplier}. */
@Warmup(iterations = 3, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class GcsClientResourceBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    ObjectStorageMock.MockServer server;

    GcsStorageSupplier storageSupplier;

    @Setup
    public void init() {
      server = mockServer(mock -> {});

      HttpTransportFactory httpTransportFactory = GcsClients.buildSharedHttpTransportFactory();

      String theToken = "the-token";
      SecretsProvider secretsProvider =
          ResolvingSecretsProvider.builder()
              .putSecretsManager(
                  "plain",
                  unsafePlainTextSecretsProvider(
                      Map.of(theToken, tokenSecret("foo", null).asMap())))
              .build();

      GcsOptions gcsOptions =
          ImmutableGcsOptions.builder()
              .defaultOptions(
                  ImmutableGcsNamedBucketOptions.builder()
                      .oauth2Token(URI.create("urn:nessie-secret:plain:" + theToken))
                      .host(server.getGcsBaseUri())
                      .build())
              .build();
      GcsConfig gcsConfig = ImmutableGcsConfig.builder().build();

      storageSupplier =
          new GcsStorageSupplier(httpTransportFactory, gcsConfig, gcsOptions, secretsProvider);
    }

    @TearDown
    public void tearDown() throws Exception {
      server.close();
    }
  }

  @Benchmark
  public void gcsClient(BenchmarkParam param, Blackhole bh) {
    GcsBucketOptions bucketOptions = param.storageSupplier.bucketOptions(StorageUri.of("gs://key"));
    bh.consume(param.storageSupplier.forLocation(bucketOptions));
  }

  @Benchmark
  public void gcsGet(BenchmarkParam param, Blackhole bh) throws IOException {
    GcsObjectIO objectIO = new GcsObjectIO(param.storageSupplier);
    try (InputStream in = objectIO.readObject(StorageUri.of("gs://bucket/key"))) {
      bh.consume(in.readAllBytes());
    }
  }

  @Benchmark
  public void gcsGet250k(BenchmarkParam param, Blackhole bh) throws IOException {
    GcsObjectIO objectIO = new GcsObjectIO(param.storageSupplier);
    try (InputStream in = objectIO.readObject(StorageUri.of("gs://bucket/s-256000"))) {
      bh.consume(in.readAllBytes());
    }
  }

  @Benchmark
  public void gcsGet4M(BenchmarkParam param, Blackhole bh) throws IOException {
    GcsObjectIO objectIO = new GcsObjectIO(param.storageSupplier);
    try (InputStream in = objectIO.readObject(StorageUri.of("gs://bucket/s-4194304"))) {
      bh.consume(in.readAllBytes());
    }
  }
}
