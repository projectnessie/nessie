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
package org.projectnessie.gc.iceberg.inttest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.collections.Int2ObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.WrappedOutputFile;
import org.projectnessie.gc.iceberg.mocks.ImmutableMockManifestFile;
import org.projectnessie.gc.iceberg.mocks.ImmutableMockManifestList;
import org.projectnessie.gc.iceberg.mocks.ImmutableMockSnapshot;
import org.projectnessie.gc.iceberg.mocks.ImmutableMockTableMetadata;
import org.projectnessie.gc.iceberg.mocks.MockManifestFile;
import org.projectnessie.gc.iceberg.mocks.MockPartitionSpec;
import org.projectnessie.gc.iceberg.mocks.MockSchema;
import org.projectnessie.gc.iceberg.mocks.MockSnapshot;
import org.projectnessie.gc.iceberg.mocks.MockTableMetadata;
import org.projectnessie.s3mock.ImmutableMockObject;
import org.projectnessie.s3mock.MockObject;
import org.projectnessie.s3mock.ObjectRetriever;
import org.projectnessie.s3mock.S3Bucket.Deleter;
import org.projectnessie.s3mock.S3Bucket.ListElement;
import org.projectnessie.s3mock.S3Bucket.Lister;
import org.projectnessie.s3mock.data.S3ObjectIdentifier;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

final class HugeFiles implements Deleter, Lister, ObjectRetriever {

  final URI baseUri = URI.create("s3://foo/");

  @Override
  public MockObject retrieve(String key) {
    BasePath base = BasePath.fromPath(URI.create(key));
    PerContent perContent = contents.get(base.contentId);
    if (perContent == null) {
      return null;
    }

    FileInfo fileInfo = new FileInfo(key);

    String tableUuid = new UUID(base.contentId.hashCode(), base.contentId.hashCode()).toString();

    MockSnapshot snapshot =
        ImmutableMockSnapshot.builder()
            .schema(MockSchema.DEFAULT_EMPTY)
            .snapshotId(fileInfo.snapshotId)
            .partitionSpec(MockPartitionSpec.DEFAULT_EMPTY)
            .tableUuid(tableUuid)
            .manifestListLocation(
                baseUri.resolve(base.base).resolve(fileInfo.asManifestListFileName()).toString())
            .build();

    MockTableMetadata metadata =
        ImmutableMockTableMetadata.builder()
            .location(baseUri.resolve(base.base).toString())
            .tableUuid(tableUuid)
            .addSnapshots(snapshot)
            .addSchemas(MockSchema.DEFAULT_EMPTY)
            .addPartitionSpecs(MockPartitionSpec.DEFAULT_EMPTY)
            .currentSchemaId(MockSchema.DEFAULT_EMPTY.schemaId())
            .defaultSpecId(MockPartitionSpec.DEFAULT_EMPTY.specId())
            .currentSnapshotId(fileInfo.snapshotId)
            .build();

    if (key.endsWith(".manifest_file.avro")) {
      WrappedOutputFile buf = new WrappedOutputFile("");

      MockManifestFile manifestFile = getManifestFile(base, fileInfo, snapshot);
      manifestFile.write(buf);
      return mockObjectForBytes(buf.asBytes());
    }

    if (key.endsWith(".manifest_list.avro")) {
      WrappedOutputFile buf = new WrappedOutputFile("");

      ImmutableMockManifestList.builder()
          .addManifestFiles(getManifestFile(base, fileInfo, snapshot))
          .build()
          .write(buf, snapshot.snapshotId(), -1L, 0L);

      return mockObjectForBytes(buf.asBytes());
    }

    if (key.endsWith(".metadata.json")) {
      return mockObjectForBytes(
          metadata.jsonNode().toString().getBytes(StandardCharsets.UTF_8), "application/json");
    }

    return null;
  }

  static MockObject mockObjectForBytes(byte[] bytes) {
    return ImmutableMockObject.builder()
        .contentLength(bytes.length)
        .writer((range, output) -> output.write(bytes))
        .build();
  }

  static MockObject mockObjectForBytes(byte[] bytes, String contentType) {
    return ImmutableMockObject.builder()
        .contentLength(bytes.length)
        .contentType(contentType)
        .writer((range, output) -> output.write(bytes))
        .build();
  }

  @NotNull
  private ImmutableMockManifestFile getManifestFile(
      BasePath base, FileInfo fileInfo, MockSnapshot snapshot) {
    return ImmutableMockManifestFile.builder()
        .path(baseUri.resolve(base.base).resolve(fileInfo.asManifestFileName(0)).toString())
        .addedFilesCount(1)
        .addedRowsCount(1L)
        .snapshotId(snapshot.snapshotId())
        .partitionSpecId(snapshot.partitionSpec().specId())
        .baseDataFilePath(baseUri.resolve(base.base).toString())
        .build();
  }

  static class BasePath {
    final int retainedSnapshots;
    final int filesPerSnapshot;
    final String contentId;
    final URI base;

    private BasePath(int retainedSnapshots, int filesPerSnapshot, String contentId, URI base) {
      this.retainedSnapshots = retainedSnapshots;
      this.filesPerSnapshot = filesPerSnapshot;
      this.contentId = contentId;
      this.base = base;
    }

    static BasePath fromPath(URI path) {
      String metaPath = path.getPath();
      int last = metaPath.lastIndexOf('/');
      int last2 = metaPath.lastIndexOf('/', last - 1);
      int last3 = metaPath.lastIndexOf('/', last2 - 1);
      int last4 = metaPath.lastIndexOf('/', last3 - 1);
      String contentId = metaPath.substring(last4 + 1, last3);
      int maxSnapshots = Integer.parseInt(metaPath.substring(last3 + 1, last2));
      int filesPerSnapshot = Integer.parseInt(metaPath.substring(last2 + 1, last));
      String basePath = metaPath.substring(0, last + 1);
      URI base = path.resolve(basePath);

      return new BasePath(maxSnapshots, filesPerSnapshot, contentId, base);
    }
  }

  static final class PerContent {
    final BitSet availableSnapshots = new BitSet();
    final Int2ObjectHashMap<BitSet> partialDeletions = new Int2ObjectHashMap<>();
  }

  final Map<String, PerContent> contents = new ConcurrentHashMap<>();

  @Override
  public Stream<ListElement> list(String prefix) {
    BasePath base = BasePath.fromPath(URI.create(prefix));
    PerContent perContent = contents.get(base.contentId);
    if (perContent == null) {
      return Stream.empty();
    }

    return IntStream.range(0, perContent.availableSnapshots.size())
        .filter(perContent.availableSnapshots::get)
        .boxed()
        .flatMap(
            snapshotId -> {
              BitSet partialDeletions = perContent.partialDeletions.get(snapshotId);

              FileInfo fileInfo = new FileInfo(snapshotId, 0);

              return Stream.concat(
                  Stream.of(
                          fileInfo.asSnapshotFileName(),
                          fileInfo.asManifestListFileName(),
                          fileInfo.asManifestFileName(0))
                      .map(
                          fileName ->
                              new ListElement() {
                                @Override
                                public String key() {
                                  return prefix + fileName;
                                }

                                @Override
                                public MockObject object() {
                                  return MockObject.builder()
                                      .lastModified(snapshotId * 1_000_000_000L)
                                      .build();
                                }
                              }),
                  IntStream.range(0, base.filesPerSnapshot)
                      .filter(f -> partialDeletions == null || !partialDeletions.get(f))
                      .mapToObj(
                          fileNum -> {
                            URI file =
                                URI.create("file-" + snapshotId + "-" + fileNum + ".data.parquet");
                            long modificationTimestamp = snapshotId * 1_000_000_000L + fileNum;
                            return new ListElement() {
                              @Override
                              public String key() {
                                return prefix + file;
                              }

                              @Override
                              public MockObject object() {
                                return MockObject.builder()
                                    .lastModified(modificationTimestamp)
                                    .build();
                              }
                            };
                          }));
            });
  }

  static class FileInfo {
    final int fileNum;
    final int snapshotId;

    FileInfo(int snapshotId, int fileNum) {
      this.snapshotId = snapshotId;
      this.fileNum = fileNum;
    }

    FileInfo(String filePath) {
      int dash = filePath.lastIndexOf('-');
      int dash2 = filePath.lastIndexOf('-', dash - 1);
      int dot = filePath.indexOf('.', dash + 1);

      int firstInt = Integer.parseInt(filePath.substring(dash + 1, dot));
      if (filePath.endsWith(".metadata.json") || filePath.endsWith(".manifest_list.avro")) {
        snapshotId = firstInt;
        fileNum = 0;
      } else {
        fileNum = firstInt;
        snapshotId = Integer.parseInt(filePath.substring(dash2 + 1, dash));
      }
    }

    String asSnapshotFileName() {
      return "meta-" + snapshotId + ".metadata.json";
    }

    String asManifestListFileName() {
      return "meta-" + snapshotId + ".manifest_list.avro";
    }

    String asManifestFileName(int num) {
      return "meta-" + snapshotId + "-" + num + ".manifest_file.avro";
    }
  }

  @Override
  public boolean delete(S3ObjectIdentifier objectIdentifier) {
    BasePath base = BasePath.fromPath(URI.create(objectIdentifier.key()));
    PerContent perContent = contents.get(base.contentId);
    if (perContent == null) {
      return true;
    }

    FileInfo fileInfo = new FileInfo(objectIdentifier.key());

    if (!perContent.availableSnapshots.get(fileInfo.snapshotId)) {
      return true;
    }

    BitSet partials =
        perContent.partialDeletions.computeIfAbsent(fileInfo.snapshotId, i -> new BitSet());
    partials.set(fileInfo.fileNum);

    if (partials.cardinality() == base.filesPerSnapshot) {
      perContent.partialDeletions.remove(fileInfo.snapshotId);
      perContent.availableSnapshots.clear(fileInfo.snapshotId);
    }

    return true;
  }

  public void createSnapshot(String contentId, int snapshotId) {
    PerContent perContent = contents.computeIfAbsent(contentId, x -> new PerContent());
    perContent.availableSnapshots.set(snapshotId);
  }
}
