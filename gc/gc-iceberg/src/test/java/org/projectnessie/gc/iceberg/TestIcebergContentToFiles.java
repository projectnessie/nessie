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
package org.projectnessie.gc.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.dataFilePath;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.manifestFileLocation;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.manifestListLocation;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.tableBase;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.tableMetadataLocation;

import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking;
import org.projectnessie.gc.iceberg.mocks.ImmutableMockSnapshot;
import org.projectnessie.gc.iceberg.mocks.ImmutableMockTableMetadata;
import org.projectnessie.gc.iceberg.mocks.MockSnapshot;
import org.projectnessie.gc.iceberg.mocks.MockTableMetadata;
import org.projectnessie.model.ContentKey;

/**
 * Unit test that uses mocked Iceberg content (metadata, manifest list, manifest file) to identify
 * all files referenced by an Iceberg snapshot.
 *
 * <p>There is an integration test that validates that Iceberg's functionality returns the same set
 * of files in the {@code nessie-gc-iceberg-inttest} project, called {@link
 * org.projectnessie.gc.iceberg.inttest.ITContentToFilesCrossCheck}.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestIcebergContentToFiles {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> contentToFiles() {
    String table1 = UUID.randomUUID().toString();
    MockSnapshot snapshot1 =
        ImmutableMockSnapshot.builder()
            .manifestListLocation(manifestListLocation(table1, 0))
            .tableUuid(table1)
            .build();
    MockTableMetadata tableMetadata1 =
        ImmutableMockTableMetadata.builder()
            .location(tableBase(table1))
            .tableUuid(table1)
            .addSnapshots(snapshot1)
            .build();
    IcebergFileIOMocking fileIO1 = IcebergFileIOMocking.forSingleSnapshot(tableMetadata1);
    ContentReference contentReference1 =
        ContentReference.icebergTable(
            "cid", "12345678", ContentKey.of("foo", "bar"), tableMetadataLocation(table1, 0), 0L);
    URI base1 = URI.create(tableBase(table1));

    Arguments args1 =
        arguments(
            fileIO1,
            contentReference1,
            ImmutableSet.of(
                URI.create(tableMetadataLocation(table1, 0)),
                URI.create(manifestListLocation(table1, 0)),
                URI.create(manifestFileLocation(table1, 0, 0)),
                URI.create(dataFilePath(table1, 0, 0, 0))),
            ImmutableSet.of(base1));

    return Stream.of(args1);
  }

  @Test
  public void checkUri() {
    soft.assertThat(
            IcebergContentToFiles.checkUri(
                "meep",
                URI.create("/foo/bar/baz"),
                ContentReference.icebergTable("a", "b", ContentKey.of("foo"), "x", 42)))
        .isEqualTo(URI.create("file:///foo/bar/baz"));
    soft.assertThat(
            IcebergContentToFiles.checkUri(
                "meep",
                URI.create("file:///foo/bar/baz"),
                ContentReference.icebergTable("a", "b", ContentKey.of("foo"), "x", 42)))
        .isEqualTo(URI.create("file:///foo/bar/baz"));
    soft.assertThat(
            IcebergContentToFiles.checkUri(
                "meep",
                URI.create("http://foo/bar/baz"),
                ContentReference.icebergTable("a", "b", ContentKey.of("foo"), "x", 42)))
        .isEqualTo(URI.create("http://foo/bar/baz"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                IcebergContentToFiles.checkUri(
                    "meep",
                    URI.create("foo/bar/baz"),
                    ContentReference.icebergTable("a", "b", ContentKey.of("foo"), "x", 42)))
        .withMessageStartingWith(
            "Iceberg content reference points to a relative meep URI '%s' as",
            URI.create("foo/bar/baz"));
  }

  @ParameterizedTest
  @MethodSource("contentToFiles")
  public void contentToFiles(
      IcebergFileIOMocking fileIO,
      ContentReference contentReference,
      Set<URI> expectedFiles,
      Set<URI> basePaths) {
    IcebergContentToFiles contentToFiles = IcebergContentToFiles.builder().io(fileIO).build();
    try (Stream<FileReference> extractFiles = contentToFiles.extractFiles(contentReference)) {
      assertThat(extractFiles)
          .allSatisfy(f -> assertThat(f.base()).isIn(basePaths))
          .allSatisfy(f -> assertThat(f.base()).extracting(URI::isAbsolute).isEqualTo(true))
          .allSatisfy(f -> assertThat(f.path()).extracting(URI::isAbsolute).isEqualTo(false))
          .allSatisfy(f -> assertThat(f.modificationTimeMillisEpoch()).isEqualTo(-1L))
          .allSatisfy(f -> assertThat(f.absolutePath()).isEqualTo(f.base().resolve(f.path())))
          .map(FileReference::absolutePath)
          .containsExactlyInAnyOrderElementsOf(expectedFiles);
    }
  }

  @Test
  public void safeAgainstMissingTrailingSlash() {
    FileIO fileIO = mock(FileIO.class);
    IcebergContentToFiles contentToFiles = IcebergContentToFiles.builder().io(fileIO).build();

    String noSlash = "file://foo/bar/baz";
    URI expectedBaseUri = URI.create(noSlash + "/");

    TableMetadata tableMetadataWithTrailingSlash = mock(TableMetadata.class);
    TableMetadata tableMetadataWithoutTrailingSlash = mock(TableMetadata.class);

    when(tableMetadataWithTrailingSlash.location()).thenReturn(noSlash + "/");
    when(tableMetadataWithoutTrailingSlash.location()).thenReturn(noSlash);

    soft.assertThat(contentToFiles.baseUri(tableMetadataWithTrailingSlash))
        .isEqualTo(expectedBaseUri);
    soft.assertThat(contentToFiles.baseUri(tableMetadataWithoutTrailingSlash))
        .isEqualTo(expectedBaseUri);
  }
}
