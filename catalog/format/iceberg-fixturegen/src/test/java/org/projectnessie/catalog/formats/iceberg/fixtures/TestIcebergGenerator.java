/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.formats.iceberg.fixtures;

import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableProperties.WRITE_METRICS_DEFAULT_TRUNCATION;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergGenerator {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @ValueSource(ints = {100, 1_000, 10_000})
  public void manyDataFiles(int numDataFiles) throws Exception {
    String avroCompression = "snappy";

    IcebergSchemaGenerator generator =
        IcebergSchemaGenerator.spec()
            .numColumns(100)
            .numPartitionColumns(3)
            .numTextColumns(20, 5, 16)
            .numTextPartitionColumns(1, 5, 10)
            .generate();

    AtomicLong manifestListSize = new AtomicLong();
    AtomicInteger manifestFiles = new AtomicInteger();
    AtomicLong manifestFileSize = new AtomicLong();

    Function<String, OutputStream> output =
        path ->
            countingNullOutput(
                path,
                (p, size) -> {
                  if (p.contains("snap-")) {
                    manifestListSize.set(size);
                  } else {
                    manifestFiles.incrementAndGet();
                    manifestFileSize.addAndGet(size);
                  }
                });

    String basePath = "file:///foo/" + randomString(1000) + "/baz/";

    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .generator(generator)
            .addDataFiles(numDataFiles)
            .avroCompression(avroCompression)
            .basePath(basePath)
            .output(output)
            .build();

    ImmutableIcebergManifestListGenerator manifestListGenerator =
        IcebergManifestListGenerator.builder()
            .generator(generator)
            .manifestFileCount(1)
            .avroCompression(avroCompression)
            .basePath(basePath)
            .output(output)
            .build();

    manifestListGenerator.generate(manifestFileGenerator.createSupplier(UUID.randomUUID()));

    System.err.println("Manifest list size: " + manifestListSize);
    System.err.println("Manifest file size: " + manifestFileSize);
  }

  @ParameterizedTest
  @ValueSource(ints = {100, 1_000, 10_000})
  public void manyManifestFiles(int numManifests) throws Exception {
    String avroCompression = "snappy";

    IcebergSchemaGenerator generator =
        IcebergSchemaGenerator.spec()
            .numColumns(100)
            .numPartitionColumns(3)
            .numTextColumns(10, 5, WRITE_METRICS_DEFAULT_TRUNCATION)
            .numTextPartitionColumns(1, 5, 10)
            .generate();

    AtomicLong manifestListSize = new AtomicLong();
    AtomicInteger manifestFiles = new AtomicInteger();
    AtomicLong manifestFileSize = new AtomicLong();

    Function<String, OutputStream> output =
        path ->
            countingNullOutput(
                path,
                (p, size) -> {
                  if (p.contains("snap-")) {
                    manifestListSize.set(size);
                  } else {
                    manifestFiles.incrementAndGet();
                    manifestFileSize.addAndGet(size);
                  }
                });

    String basePath = "file:///foo/" + randomString(1000) + "/baz/";

    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .generator(generator)
            .addDataFiles(2)
            .avroCompression(avroCompression)
            .basePath(basePath)
            .output(output)
            .build();

    ImmutableIcebergManifestListGenerator manifestListGenerator =
        IcebergManifestListGenerator.builder()
            .generator(generator)
            .manifestFileCount(numManifests)
            .avroCompression(avroCompression)
            .basePath(basePath)
            .output(output)
            .build();

    manifestListGenerator.generate(manifestFileGenerator.createSupplier(UUID.randomUUID()));

    System.err.println("Manifest list size: " + manifestListSize);
    System.err.printf(
        "Manifest files: %d - average size: %d%n",
        manifestFiles.get(), manifestFileSize.get() / manifestFiles.get());
  }

  static OutputStream countingNullOutput(String path, BiConsumer<String, Long> fileSize) {
    return new OutputStream() {
      long size = 0;

      @Override
      public void write(int b) {
        size++;
      }

      @Override
      public void write(byte[] b, int off, int len) {
        size += len;
      }

      @Override
      public void close() {
        fileSize.accept(path, size);
      }
    };
  }

  static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    for (int i = 0; i < len; i++) {
      sb.append((char) (' ' + rand.nextInt(95)));
    }
    return sb.toString();
  }

  @ParameterizedTest
  @ValueSource(strings = {"gzip", "snappy", "zstd", "uncompressed"})
  public void compressions(String avroCompression) throws Exception {
    IcebergSchemaGenerator generator =
        IcebergSchemaGenerator.spec()
            .numColumns(10)
            .numPartitionColumns(3)
            .numTextColumns(10, 10, WRITE_METRICS_DEFAULT_TRUNCATION)
            .numTextPartitionColumns(1, 20, 30)
            .generate();

    AtomicLong manifestListSize = new AtomicLong();
    AtomicInteger manifestFiles = new AtomicInteger();
    AtomicLong manifestFileSize = new AtomicLong();

    Function<String, OutputStream> output =
        path ->
            countingNullOutput(
                path,
                (p, size) -> {
                  if (p.contains("snap-")) {
                    manifestListSize.set(size);
                  } else {
                    manifestFiles.incrementAndGet();
                    manifestFileSize.addAndGet(size);
                  }
                });

    String basePath = "file:///foo/bar/baz/";

    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .generator(generator)
            .addDataFiles(100)
            .avroCompression(avroCompression)
            .basePath(basePath)
            .output(output)
            .build();

    ImmutableIcebergManifestListGenerator manifestListGenerator =
        IcebergManifestListGenerator.builder()
            .generator(generator)
            .manifestFileCount(100)
            .avroCompression(avroCompression)
            .basePath(basePath)
            .output(output)
            .build();

    manifestListGenerator.generate(manifestFileGenerator.createSupplier(UUID.randomUUID()));

    System.err.println("Manifest list size: " + manifestListSize);
    System.err.printf(
        "Manifest files: %d - average size: %d%n",
        manifestFiles.get(), manifestFileSize.get() / manifestFiles.get());
  }

  @Test
  public void testGeneratorRealFiles(@TempDir Path tempDir) throws Exception {
    IcebergSchemaGenerator generator =
        IcebergSchemaGenerator.spec()
            .numColumns(10)
            .numPartitionColumns(3)
            .numTextColumns(3, 10, WRITE_METRICS_DEFAULT_TRUNCATION)
            .numTextPartitionColumns(1, 20, 30)
            .generate();

    String basePath = tempDir.toUri().toString();

    Function<String, OutputStream> output =
        path -> {
          Path f = Paths.get(URI.create(path));
          try {
            return Files.newOutputStream(f, StandardOpenOption.CREATE_NEW);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };

    IcebergManifestFileGenerator manifestFileGenerator =
        IcebergManifestFileGenerator.builder()
            .generator(generator)
            .addDataFiles(20)
            .basePath(basePath)
            .output(output)
            .build();

    ImmutableIcebergManifestListGenerator manifestListGenerator =
        IcebergManifestListGenerator.builder()
            .generator(generator)
            .manifestFileCount(20)
            .basePath(basePath)
            .output(output)
            .build();

    manifestListGenerator.generate(manifestFileGenerator.createSupplier(UUID.randomUUID()));

    System.err.println(generator);
  }
}
