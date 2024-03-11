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
import static org.mockito.ArgumentMatchers.any;
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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
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
  public void safeAgainstMissingTableMetadata() {
    InputFile inputFile = mock(InputFile.class);
    FileIO fileIO = mock(FileIO.class);

    when(inputFile.location()).thenReturn("/blah.metadata.json");
    when(inputFile.newStream()).thenThrow(new NotFoundException("mocked"));
    when(fileIO.newInputFile(any(String.class))).thenReturn(inputFile);

    IcebergContentToFiles contentToFiles = IcebergContentToFiles.builder().io(fileIO).build();
    try (Stream<FileReference> extractFiles =
        contentToFiles.extractFiles(
            ContentReference.icebergTable(
                "cid", "1234", ContentKey.of("foo"), "/blah.metadata.json", 42L))) {
      assertThat(extractFiles).isEmpty();
    }
  }

  @Test
  public void safeAgainstMissingTrailingSlash() {
    String noSlash = "file:///foo/bar/baz";
    URI expectedBaseUri = URI.create(noSlash + "/");

    TableMetadata tableMetadataWithTrailingSlash = mock(TableMetadata.class);
    TableMetadata tableMetadataWithoutTrailingSlash = mock(TableMetadata.class);

    when(tableMetadataWithTrailingSlash.location()).thenReturn(noSlash + "/");
    when(tableMetadataWithoutTrailingSlash.location()).thenReturn(noSlash);

    ContentReference contentReference =
        ContentReference.icebergTable("cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    soft.assertThat(IcebergContentToFiles.baseUri(tableMetadataWithTrailingSlash, contentReference))
        .isEqualTo(expectedBaseUri);
    soft.assertThat(
            IcebergContentToFiles.baseUri(tableMetadataWithoutTrailingSlash, contentReference))
        .isEqualTo(expectedBaseUri);
  }

  @Test
  public void checkUri() {
    ContentReference contentReference =
        ContentReference.icebergTable("a", "b", ContentKey.of("foo"), "x", 42);

    soft.assertThat(
            IcebergContentToFiles.checkUri("meep", URI.create("/foo/bar/baz"), contentReference))
        .isEqualTo(URI.create("file:///foo/bar/baz"));
    soft.assertThat(
            IcebergContentToFiles.checkUri(
                "meep", URI.create("file:///foo/bar/baz"), contentReference))
        .isEqualTo(URI.create("file:///foo/bar/baz"));
    soft.assertThat(
            IcebergContentToFiles.checkUri(
                "meep", URI.create("http://foo/bar/baz"), contentReference))
        .isEqualTo(URI.create("http://foo/bar/baz"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                IcebergContentToFiles.checkUri("meep", URI.create("foo/bar/baz"), contentReference))
        .withMessage(
            "Iceberg content reference points to the meep URI '%s' as content-key foo on commit b without a scheme and with a relative path, which is not supported.",
            URI.create("foo/bar/baz"));

    // Note: the following is a completely valid file-scheme URI, pointing to the root directory
    // on the host "location"!
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                IcebergContentToFiles.checkUri(
                    "meep", URI.create("file://location"), contentReference))
        .withMessageContaining("points to the host-specific meep URI ");

    soft.assertThat(
            IcebergContentToFiles.checkUri("meep", URI.create("file:/location"), contentReference))
        .isEqualTo(URI.create("file:///location"));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> IcebergContentToFiles.checkUri("meep", URI.create("location"), contentReference))
        .withMessageContaining("points to the meep URI ")
        .withMessageContaining("without a scheme and with a relative path, which is not supported");

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                IcebergContentToFiles.checkUri(
                    "meep", URI.create("file:location"), contentReference))
        .withMessageContaining("points to the meep URI ")
        .withMessageContaining("with a non-absolute scheme-specific-part location");
  }

  @Test
  public void elementaryUrisFromSnapshot() {
    ContentReference contentReference =
        ContentReference.icebergTable("cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(null, contentReference))
        .containsExactly(URI.create("file:///table-metadata"));

    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.manifestListLocation()).thenReturn(null);
    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(snapshot, contentReference))
        .containsExactly(URI.create("file:///table-metadata"));

    snapshot = mock(Snapshot.class);
    when(snapshot.manifestListLocation()).thenReturn("/manifest-list");
    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(snapshot, contentReference))
        .containsExactlyInAnyOrder(
            URI.create("file:///table-metadata"), URI.create("file:///manifest-list"));

    snapshot = mock(Snapshot.class);
    when(snapshot.manifestListLocation()).thenReturn("meep://manifest-list");
    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(snapshot, contentReference))
        .containsExactlyInAnyOrder(
            URI.create("file:///table-metadata"), URI.create("meep://manifest-list"));
  }

  @Test
  public void checkUriForBase() {
    ContentReference contentReference =
        ContentReference.icebergTable("cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("meep://location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(URI.create("meep://location/"));

    metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("/location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(URI.create("file:///location/"));

    metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("file:/location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(URI.create("file:///location/"));

    metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("file:///location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(URI.create("file:///location/"));
  }

  @Test
  public void checkUriForManifest() {
    ContentReference contentReference =
        ContentReference.icebergTable("cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    ManifestFile manifestFile = mock(ManifestFile.class);
    when(manifestFile.path()).thenReturn("file:/foo/bar");
    soft.assertThat(IcebergContentToFiles.manifestFileUri(manifestFile, contentReference))
        .isEqualTo(URI.create("file:///foo/bar"));

    manifestFile = mock(ManifestFile.class);
    when(manifestFile.path()).thenReturn("file:///foo/bar");
    soft.assertThat(IcebergContentToFiles.manifestFileUri(manifestFile, contentReference))
        .isEqualTo(URI.create("file:///foo/bar"));

    manifestFile = mock(ManifestFile.class);
    when(manifestFile.path()).thenReturn("/foo/bar");
    soft.assertThat(IcebergContentToFiles.manifestFileUri(manifestFile, contentReference))
        .isEqualTo(URI.create("file:///foo/bar"));

    ManifestFile manifestFile2 = mock(ManifestFile.class);
    when(manifestFile2.path()).thenReturn(null);
    soft.assertThatNullPointerException()
        .isThrownBy(() -> IcebergContentToFiles.manifestFileUri(manifestFile2, contentReference))
        .withMessageStartingWith("Iceberg manifest file is expected to have a non-null path for");
  }

  @Test
  public void checkUriForDataFile() {
    ContentReference contentReference =
        ContentReference.icebergTable("cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    soft.assertThat(IcebergContentToFiles.dataFileUri("file:/foo/bar", contentReference))
        .isEqualTo(URI.create("file:///foo/bar"));

    soft.assertThat(IcebergContentToFiles.dataFileUri("file:///foo/bar", contentReference))
        .isEqualTo(URI.create("file:///foo/bar"));

    soft.assertThat(IcebergContentToFiles.dataFileUri("/foo/bar", contentReference))
        .isEqualTo(URI.create("file:///foo/bar"));
  }
}
