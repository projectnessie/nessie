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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.dataFilePath;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.manifestFileLocation;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.manifestListLocation;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.tableBase;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.tableMetadataLocation;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.viewMetadataLocation;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
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
import org.projectnessie.storage.uri.StorageUri;

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
    String tableId = UUID.randomUUID().toString();
    String tableMetaLocation = tableMetadataLocation(tableId, 0);
    MockSnapshot tableSnapshot =
        ImmutableMockSnapshot.builder()
            .manifestListLocation(manifestListLocation(tableId, 0))
            .tableUuid(tableId)
            .build();
    MockTableMetadata tableMetadata =
        ImmutableMockTableMetadata.builder()
            .location(tableBase(tableId))
            .tableUuid(tableId)
            .addSnapshots(tableSnapshot)
            .build();
    IcebergFileIOMocking fileIO1 = IcebergFileIOMocking.forSingleSnapshot(tableMetadata);
    ContentReference contentReferenceTable =
        icebergContent(
            ICEBERG_TABLE, "cid", "12345678", ContentKey.of("foo", "bar"), tableMetaLocation, 0L);
    StorageUri baseTable = StorageUri.of(tableBase(tableId));

    String viewId = UUID.randomUUID().toString();
    String viewMetaLocation = viewMetadataLocation(viewId, 42);
    Schema viewSchema =
        new Schema(1, singletonList(Types.NestedField.required(1, "foo", Types.IntegerType.get())));
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .assignUUID(viewId)
            .addSchema(viewSchema)
            .setCurrentVersion(
                ImmutableViewVersion.builder()
                    .versionId(42)
                    .schemaId(viewSchema.schemaId())
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("SELECT")
                            .dialect("nessie")
                            .build())
                    .timestampMillis(0L)
                    .defaultNamespace(Namespace.of("foo"))
                    .build(),
                viewSchema)
            .setLocation(tableBase(viewId))
            .build();
    IcebergFileIOMocking fileIO2 = IcebergFileIOMocking.forSingleVersion(viewMetadata);
    ContentReference contentReferenceView =
        icebergContent(
            ICEBERG_VIEW, "cid", "12345678", ContentKey.of("foo", "bar"), viewMetaLocation, 0L);
    StorageUri baseView = StorageUri.of(tableBase(viewId));

    Arguments argsTable =
        arguments(
            fileIO1,
            contentReferenceTable,
            ImmutableSet.of(
                StorageUri.of(tableMetaLocation),
                StorageUri.of(manifestListLocation(tableId, 0)),
                StorageUri.of(manifestFileLocation(tableId, 0, 0)),
                StorageUri.of(dataFilePath(tableId, 0, 0, 0))),
            ImmutableSet.of(baseTable));

    Arguments argsView =
        arguments(
            fileIO2,
            contentReferenceView,
            ImmutableSet.of(StorageUri.of(viewMetaLocation)),
            ImmutableSet.of(baseView));

    return Stream.of(argsTable, argsView);
  }

  @ParameterizedTest
  @MethodSource("contentToFiles")
  public void contentToFiles(
      IcebergFileIOMocking fileIO,
      ContentReference contentReference,
      Set<StorageUri> expectedFiles,
      Set<StorageUri> basePaths) {
    IcebergContentToFiles contentToFiles = IcebergContentToFiles.builder().io(fileIO).build();
    try (Stream<FileReference> extractFiles = contentToFiles.extractFiles(contentReference)) {
      assertThat(extractFiles)
          .allSatisfy(f -> assertThat(f.base()).isIn(basePaths))
          .allSatisfy(f -> assertThat(f.base()).extracting(StorageUri::scheme).isNotNull())
          .allSatisfy(f -> assertThat(f.path()).extracting(StorageUri::scheme).isNull())
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
            icebergContent(
                ICEBERG_TABLE, "cid", "1234", ContentKey.of("foo"), "/blah.metadata.json", 42L))) {
      assertThat(extractFiles).isEmpty();
    }
  }

  @Test
  public void safeAgainstMissingTrailingSlash() {
    String noSlash = "file:///foo/bar/baz";
    StorageUri expectedBaseUri = StorageUri.of(noSlash + "/");

    TableMetadata tableMetadataWithTrailingSlash = mock(TableMetadata.class);
    TableMetadata tableMetadataWithoutTrailingSlash = mock(TableMetadata.class);

    when(tableMetadataWithTrailingSlash.location()).thenReturn(noSlash + "/");
    when(tableMetadataWithoutTrailingSlash.location()).thenReturn(noSlash);

    ContentReference contentReference =
        icebergContent(ICEBERG_TABLE, "cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    soft.assertThat(IcebergContentToFiles.baseUri(tableMetadataWithTrailingSlash, contentReference))
        .isEqualTo(expectedBaseUri);
    soft.assertThat(
            IcebergContentToFiles.baseUri(tableMetadataWithoutTrailingSlash, contentReference))
        .isEqualTo(expectedBaseUri);
  }

  @Test
  public void checkUri() {
    ContentReference contentReference =
        icebergContent(ICEBERG_TABLE, "a", "b", ContentKey.of("foo"), "x", 42);

    soft.assertThat(IcebergContentToFiles.checkUri("meep", "/foo/bar/baz", contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar/baz"));
    soft.assertThat(IcebergContentToFiles.checkUri("meep", "file:///foo/bar/baz", contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar/baz"));
    soft.assertThat(IcebergContentToFiles.checkUri("meep", "http://foo/bar/baz", contentReference))
        .isEqualTo(StorageUri.of("http://foo/bar/baz"));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> IcebergContentToFiles.checkUri("meep", "foo/bar/baz", contentReference))
        .withMessage(
            "Iceberg content reference points to the meep URI '%s' as content-key foo on commit b without a scheme and with a relative path, which is not supported.",
            StorageUri.of("foo/bar/baz"));

    // Note: the following is a completely valid file-scheme URI, pointing to the root directory
    // on the host "location"!
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () -> IcebergContentToFiles.checkUri("meep", "file://location", contentReference))
        .withMessageContaining("points to the host-specific meep URI ");

    soft.assertThat(IcebergContentToFiles.checkUri("meep", "file:/location", contentReference))
        .isEqualTo(StorageUri.of("file:///location"));

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> IcebergContentToFiles.checkUri("meep", "location", contentReference))
        .withMessageContaining("points to the meep URI ")
        .withMessageContaining("without a scheme and with a relative path, which is not supported");

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> IcebergContentToFiles.checkUri("meep", "file:location", contentReference))
        .withMessageContaining("points to the meep URI ")
        .withMessageContaining("with a non-absolute scheme-specific-part location");
  }

  @Test
  public void elementaryUrisFromSnapshot() {
    ContentReference contentReference =
        icebergContent(ICEBERG_TABLE, "cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(null, contentReference))
        .containsExactly(StorageUri.of("file:///table-metadata"));

    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.manifestListLocation()).thenReturn(null);
    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(snapshot, contentReference))
        .containsExactly(StorageUri.of("file:///table-metadata"));

    snapshot = mock(Snapshot.class);
    when(snapshot.manifestListLocation()).thenReturn("/manifest-list");
    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(snapshot, contentReference))
        .containsExactlyInAnyOrder(
            StorageUri.of("file:///table-metadata"), StorageUri.of("file:///manifest-list"));

    snapshot = mock(Snapshot.class);
    when(snapshot.manifestListLocation()).thenReturn("meep://manifest-list");
    soft.assertThat(IcebergContentToFiles.elementaryUrisFromSnapshot(snapshot, contentReference))
        .containsExactlyInAnyOrder(
            StorageUri.of("file:///table-metadata"), StorageUri.of("meep://manifest-list"));
  }

  @Test
  public void checkUriForBase() {
    ContentReference contentReference =
        icebergContent(ICEBERG_TABLE, "cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("meep://location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(StorageUri.of("meep://location/"));

    metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("/location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(StorageUri.of("file:///location/"));

    metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("file:/location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(StorageUri.of("file:///location/"));

    metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("file:///location");
    soft.assertThat(IcebergContentToFiles.baseUri(metadata, contentReference))
        .isEqualTo(StorageUri.of("file:///location/"));
  }

  @Test
  public void checkUriForManifest() {
    ContentReference contentReference =
        icebergContent(ICEBERG_TABLE, "cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    ManifestFile manifestFile = mock(ManifestFile.class);
    when(manifestFile.path()).thenReturn("file:/foo/bar");
    soft.assertThat(IcebergContentToFiles.manifestFileUri(manifestFile, contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar"));

    manifestFile = mock(ManifestFile.class);
    when(manifestFile.path()).thenReturn("file:///foo/bar");
    soft.assertThat(IcebergContentToFiles.manifestFileUri(manifestFile, contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar"));

    manifestFile = mock(ManifestFile.class);
    when(manifestFile.path()).thenReturn("/foo/bar");
    soft.assertThat(IcebergContentToFiles.manifestFileUri(manifestFile, contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar"));

    ManifestFile manifestFile2 = mock(ManifestFile.class);
    when(manifestFile2.path()).thenReturn(null);
    soft.assertThatNullPointerException()
        .isThrownBy(() -> IcebergContentToFiles.manifestFileUri(manifestFile2, contentReference))
        .withMessageStartingWith("Iceberg manifest file is expected to have a non-null path for");
  }

  @Test
  public void checkUriForDataFile() {
    ContentReference contentReference =
        icebergContent(ICEBERG_TABLE, "cid", "abcd", ContentKey.of("abc"), "/table-metadata", 42L);

    soft.assertThat(IcebergContentToFiles.dataFileUri("file:/foo/bar", contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar"));

    soft.assertThat(IcebergContentToFiles.dataFileUri("file:///foo/bar", contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar"));

    soft.assertThat(IcebergContentToFiles.dataFileUri("/foo/bar", contentReference))
        .isEqualTo(StorageUri.of("file:///foo/bar"));
  }
}
