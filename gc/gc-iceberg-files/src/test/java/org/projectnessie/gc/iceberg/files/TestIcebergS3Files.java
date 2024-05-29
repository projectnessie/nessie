/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.iceberg.files;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.objectstoragemock.Bucket;
import org.projectnessie.objectstoragemock.Bucket.ListElement;
import org.projectnessie.objectstoragemock.MockObject;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.objectstoragemock.ObjectStorageMock.MockServer;
import org.projectnessie.storage.uri.StorageUri;

public class TestIcebergS3Files {

  private static final String BUCKET = "bucket";

  @Test
  public void icebergS3() throws Exception {
    StorageUri baseUri = icebergBaseUri("/path/");

    Set<String> keys = new TreeSet<>();
    keys.add("path/file-1");
    keys.add("path/file-2");
    keys.add("path/file-3");
    keys.add("path/dir-1/file-4");
    keys.add("path/dir-1/dir-2/file-5");

    try (MockServer server = createServer(keys);
        IcebergFiles s3 = createIcebergFiles(server)) {

      Set<StorageUri> expect =
          keys.stream()
              .map(TestIcebergS3Files::icebergBaseUri)
              .collect(Collectors.toCollection(HashSet::new));

      try (Stream<FileReference> files = s3.listRecursively(baseUri)) {
        assertThat(files)
            .allSatisfy(f -> assertThat(f.base()).isEqualTo(baseUri))
            .map(FileReference::absolutePath)
            .containsExactlyInAnyOrderElementsOf(expect);
      }

      s3.deleteMultiple(
          baseUri,
          Stream.of(
              FileReference.of(StorageUri.of("file-2"), baseUri, -1L),
              FileReference.of(StorageUri.of("file-3"), baseUri, -1L)));
      expect.remove(baseUri.resolve("file-2"));
      expect.remove(baseUri.resolve("file-3"));

      try (Stream<FileReference> files = s3.listRecursively(baseUri)) {
        assertThat(files)
            .allSatisfy(f -> assertThat(f.base()).isEqualTo(baseUri))
            .map(FileReference::absolutePath)
            .containsExactlyInAnyOrderElementsOf(expect);
      }

      s3.delete(FileReference.of(StorageUri.of("dir-1/file-4"), baseUri, -1L));
      expect.remove(baseUri.resolve("dir-1/file-4"));

      try (Stream<FileReference> files = s3.listRecursively(baseUri)) {
        assertThat(files)
            .allSatisfy(f -> assertThat(f.base()).isEqualTo(baseUri))
            .map(FileReference::absolutePath)
            .containsExactlyInAnyOrderElementsOf(expect);
      }
    }
  }

  /**
   * Creates many files, lists the files, deletes 10% of the created files, lists again.
   *
   * <p>Minio in the used configuration is not particularly fast - creating 100000 objects with 4
   * threads (more crashes w/ timeouts) takes about ~30 minutes (plus ~3 seconds for listing 100000
   * objects, plus ~3 seconds for deleting 10000 objects).
   */
  @ParameterizedTest
  @ValueSource(ints = {500})
  public void manyFiles(int numFiles) throws Exception {
    StorageUri baseUri = icebergBaseUri("/path/");

    Set<String> keys =
        IntStream.range(0, numFiles)
            .mapToObj(i -> String.format("path/%d/%d", i % 100, i))
            .collect(Collectors.toCollection(HashSet::new));

    try (MockServer server = createServer(keys);
        IcebergFiles s3 = createIcebergFiles(server)) {

      try (Stream<FileReference> files = s3.listRecursively(baseUri)) {
        assertThat(files).hasSize(numFiles);
      }

      int deletes = numFiles / 10;
      assertThat(
              s3.deleteMultiple(
                  baseUri,
                  IntStream.range(0, deletes)
                      .mapToObj(i -> StorageUri.of(String.format("%d/%d", i % 100, i)))
                      .map(p -> FileReference.of(p, baseUri, -1L))))
          .isEqualTo(DeleteSummary.of(deletes, 0L));

      try (Stream<FileReference> files = s3.listRecursively(baseUri)) {
        assertThat(files).hasSize(numFiles - deletes);
      }
    }
  }

  private IcebergFiles createIcebergFiles(MockServer server) {
    return IcebergFiles.builder()
        .properties(server.icebergProperties())
        .hadoopConfiguration(hadoopConfiguration(server))
        .build();
  }

  private static MockServer createServer(Set<String> keys) {
    return ObjectStorageMock.builder()
        .putBuckets(
            BUCKET,
            Bucket.builder()
                .lister(
                    (String prefix, String offset) ->
                        keys.stream()
                            .map(
                                key ->
                                    new ListElement() {
                                      @Override
                                      public String key() {
                                        return key;
                                      }

                                      @Override
                                      public MockObject object() {
                                        return MockObject.builder().build();
                                      }
                                    }))
                .deleter(keys::remove)
                .build())
        .build()
        .start();
  }

  protected static StorageUri icebergBaseUri(String path) {
    return StorageUri.of(String.format("s3://%s/", BUCKET)).resolve(path);
  }

  protected Configuration hadoopConfiguration(MockServer server) {
    Configuration conf = new Configuration();
    server.hadoopConfiguration().forEach(conf::set);
    return conf;
  }
}
