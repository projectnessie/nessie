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

import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;
import static org.projectnessie.storage.uri.StorageUri.SCHEME_FILE;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReaderUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.immutables.value.Value;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.expire.ContentToFiles;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.model.Content;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality to extract information about files and base locations from Iceberg
 * content, which is the one Iceberg table snapshot referred to by a Nessie commit.
 */
@Value.Immutable
public abstract class IcebergContentToFiles implements ContentToFiles {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergContentToFiles.class);

  static final String ICEBERG_NOT_FOUND_EXCEPTION =
      "org.apache.iceberg.exceptions.NotFoundException";
  static final String S3_KEY_NOT_FOUND_EXCEPTION =
      "software.amazon.awssdk.services.s3.model.NoSuchKeyException";
  static final String GCS_STORAGE_EXCEPTION = "com.google.cloud.storage.StorageException";
  static final String ADLS_BLOB_STORAGE_EXCEPTION =
      "com.azure.storage.blob.models.BlobStorageException";

  static final String GCS_NOT_FOUND_START = "404 Not Found";

  static final String ADLS_PATH_NOT_FOUND_CODE = "PathNotFound";
  static final String ADLS_BLOB_NOT_FOUND_CODE = "BlobNotFound";

  public static Builder builder() {
    return ImmutableIcebergContentToFiles.builder();
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder io(FileIO io);

    IcebergContentToFiles build();
  }

  abstract FileIO io();

  /**
   * Provides a {@link Stream} with the {@link FileReference}s referencing the table-metadata, the
   * {@link Snapshot#manifestListLocation() manifest-list}, all {@link ManifestFile manifest-files}
   * and all {@link org.apache.iceberg.DataFile data files}.
   */
  @Override
  @MustBeClosed
  public Stream<FileReference> extractFiles(ContentReference contentReference) {
    Content.Type contentType = contentReference.contentType();
    if (contentType.equals(ICEBERG_TABLE)) {
      return extractTableFiles(contentReference);
    } else if (contentType.equals(ICEBERG_VIEW)) {
      return extractViewFiles(contentReference);
    } else {
      throw new IllegalArgumentException(
          "Expect ICEBERG_TABLE/ICEBERG_VIEW, but got " + contentType);
    }
  }

  private Stream<FileReference> extractViewFiles(ContentReference contentReference) {
    FileIO io = io();

    ViewMetadata viewMetadata;
    try {
      viewMetadata = ViewMetadataParser.read(io.newInputFile(contentReference.metadataLocation()));
    } catch (Exception notFoundCandidate) {
      return handleNotFound(contentReference, notFoundCandidate, "View");
    }

    Stream<StorageUri> allFiles = Stream.of(metadataStorageUri(contentReference));

    StorageUri baseUri = baseUri(viewMetadata, contentReference);

    return extractFilesRelativize(allFiles, baseUri);
  }

  private Stream<FileReference> extractTableFiles(ContentReference contentReference) {
    FileIO io = io();

    TableMetadata tableMetadata;
    try {
      tableMetadata = TableMetadataParser.read(io, contentReference.metadataLocation());
    } catch (Exception notFoundCandidate) {
      return handleNotFound(contentReference, notFoundCandidate, "Table");
    }

    long snapshotId =
        Objects.requireNonNull(
            contentReference.snapshotId(),
            "Iceberg content is expected to have a non-null snapshot-ID");

    Snapshot snapshot =
        snapshotId < 0L ? tableMetadata.currentSnapshot() : tableMetadata.snapshot(snapshotId);

    Stream<StorageUri> allFiles = elementaryUrisFromSnapshot(snapshot, contentReference);

    if (snapshot != null) {
      long effectiveSnapshotId = snapshot.snapshotId();
      allFiles =
          Stream.concat(
              allFiles,
              tableMetadata.statisticsFiles().stream()
                  .filter(s -> s.snapshotId() == effectiveSnapshotId)
                  .map(StatisticsFile::path)
                  .map(StorageUri::of));
      allFiles =
          Stream.concat(
              allFiles,
              tableMetadata.partitionStatisticsFiles().stream()
                  .filter(s -> s.snapshotId() == effectiveSnapshotId)
                  .map(PartitionStatisticsFile::path)
                  .map(StorageUri::of));

      Map<Integer, PartitionSpec> specsById = tableMetadata.specsById();
      allFiles =
          Stream.concat(
              allFiles,
              Stream.of("")
                  // .flatMap() for lazy loading
                  .flatMap(
                      x -> {
                        try {
                          @SuppressWarnings("MustBeClosedChecker")
                          Stream<StorageUri> r =
                              allManifestsAndDataFiles(io, snapshot, specsById, contentReference);
                          return r;
                        } catch (Exception e) {
                          String msg =
                              "Failed to get manifest files for "
                                  + contentReference.contentType()
                                  + " "
                                  + contentReference.contentKey()
                                  + ", content-ID "
                                  + contentReference.contentId()
                                  + " at commit "
                                  + contentReference.commitId()
                                  + " via "
                                  + contentReference.metadataLocation();
                          LOGGER.error("{}", msg, e);
                          throw new RuntimeException(msg, e);
                        }
                      }));
    }

    StorageUri baseUri = baseUri(tableMetadata, contentReference);

    return extractFilesRelativize(allFiles, baseUri);
  }

  private static Stream<FileReference> extractFilesRelativize(
      Stream<StorageUri> allFiles, StorageUri baseUri) {
    return allFiles.map(baseUri::relativize).map(u -> FileReference.of(u, baseUri, -1L));
  }

  private static Stream<FileReference> handleNotFound(
      ContentReference contentReference, Exception notFoundCandidate, String kind) {
    boolean notFound = false;
    for (Throwable c = notFoundCandidate; c != null; c = c.getCause()) {
      switch (c.getClass().getName()) {
        case ICEBERG_NOT_FOUND_EXCEPTION:
        case S3_KEY_NOT_FOUND_EXCEPTION:
          notFound = true;
          break;
        case GCS_STORAGE_EXCEPTION:
          if (c.getMessage().startsWith(GCS_NOT_FOUND_START)) {
            notFound = true;
          }
          break;
        case ADLS_BLOB_STORAGE_EXCEPTION:
          String message = c.getMessage();
          if (message.contains(ADLS_PATH_NOT_FOUND_CODE)
              || message.contains(ADLS_BLOB_NOT_FOUND_CODE)) {
            notFound = true;
          }
          break;
        default:
          break;
      }

      if (notFound || c == c.getCause()) {
        break;
      }
    }

    if (notFound) {
      // It is safe to assume that a missing table-metadata means no referenced files.
      // A table-metadata can be missing, because a previous Nessie GC "sweep" phase deleted it.
      LOGGER.info(
          "{} metadata {} for snapshot ID {} for content-key {} at Nessie commit {} does not exist, probably already deleted, assuming no files",
          kind,
          contentReference.metadataLocation(),
          contentReference.snapshotId(),
          contentReference.contentKey(),
          contentReference.commitId());
      return Stream.empty();
    }

    String msg =
        "Failed to extract content of "
            + contentReference.contentType()
            + " "
            + contentReference.contentKey()
            + ", content-ID "
            + contentReference.contentId()
            + " at commit "
            + contentReference.commitId()
            + " via "
            + contentReference.metadataLocation();
    LOGGER.error("{}", msg, notFoundCandidate);
    throw new RuntimeException(msg, notFoundCandidate);
  }

  /**
   * For the given {@link Snapshot}, provide a {@link Stream} of all manifest files with {@link
   * #allDataAndDeleteFiles(FileIO, Map, ManifestFile, ContentReference) all included data and
   * delete files}.
   */
  @MustBeClosed
  static Stream<StorageUri> allManifestsAndDataFiles(
      FileIO io,
      Snapshot snapshot,
      Map<Integer, PartitionSpec> specsById,
      ContentReference contentReference) {
    return allManifests(io, specsById, snapshot)
        .flatMap(
            mf -> {
              StorageUri manifestFileLoc = manifestFileUri(mf, contentReference);
              @SuppressWarnings("MustBeClosedChecker")
              Stream<StorageUri> allDataAndDeleteFiles =
                  allDataAndDeleteFiles(io, specsById, mf, contentReference);
              return Stream.concat(Stream.of(manifestFileLoc), allDataAndDeleteFiles);
            });
  }

  /** Provide all {@link ManifestFile}s for the given {@link Snapshot}. */
  static Stream<ManifestFile> allManifests(
      FileIO io, Map<Integer, PartitionSpec> specsById, Snapshot snapshot) {
    return snapshot.allManifests(io).stream();
  }

  /**
   * For the given {@link ManifestFile}, provide a {@link Stream} of <em>all</em> data and delete
   * files, means including all {@link org.apache.iceberg.ManifestEntry}s of every status ({@code
   * EXISTING}, {@code ADDED}, {@code DELETED}.
   */
  @MustBeClosed
  static Stream<StorageUri> allDataAndDeleteFiles(
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      ManifestFile manifestFile,
      ContentReference contentReference) {
    CloseableIterable<String> iter;
    try {
      iter = ManifestReaderUtil.readPathsFromManifest(manifestFile, specsById, io);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to get paths from manifest file " + manifestFile.path(), e);
    }
    return StreamSupport.stream(iter.spliterator(), false)
        .map(dataFilePath -> dataFileUri(dataFilePath, contentReference))
        .onClose(
            () -> {
              try {
                iter.close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  /**
   * All processed {@link StorageUri}s must have a schema part and, if the schema is {@code file},
   * the path must be an absolute path.
   */
  static StorageUri checkUri(String type, String location, ContentReference contentReference) {
    StorageUri loc = StorageUri.of(location);
    if (loc.scheme() == null) {
      Preconditions.checkArgument(
          location.startsWith("/"),
          "Iceberg content reference points to the %s URI '%s' as content-key %s on commit %s without a scheme and with a relative path, which is not supported.",
          type,
          location,
          contentReference.contentKey(),
          contentReference.commitId());
      return StorageUri.of("file://" + location);
    }

    if (SCHEME_FILE.equals(loc.scheme())) {
      URI uri = URI.create(location);
      Preconditions.checkArgument(
          uri.getSchemeSpecificPart().startsWith("/"),
          "Iceberg content reference points to the %s URI '%s' as content-key %s on commit %s with a non-absolute scheme-specific-part %s, which is not supported.",
          type,
          uri,
          contentReference.contentKey(),
          contentReference.commitId(),
          uri.getSchemeSpecificPart());

      Preconditions.checkArgument(
          uri.getHost() == null,
          "Iceberg content reference points to the host-specific %s URI '%s' as content-key %s on commit %s without a scheme, which is not supported.",
          type,
          location,
          contentReference.contentKey(),
          contentReference.commitId());
    }
    return loc;
  }

  static Stream<StorageUri> elementaryUrisFromSnapshot(
      Snapshot snapshot, ContentReference contentReference) {
    StorageUri metadataLoc = metadataStorageUri(contentReference);

    if (snapshot == null) {
      return Stream.of(metadataLoc);
    }

    String manifestListLocation = snapshot.manifestListLocation();
    if (manifestListLocation == null) {
      // Iceberg spec v1 has the manifest files embedded in the table-metadata, Iceberg spec v2
      // has a separate manifest list file.
      return Stream.of(metadataLoc);
    }

    StorageUri manifestListLoc =
        checkUri("manifest list", snapshot.manifestListLocation(), contentReference);

    return Stream.of(metadataLoc, manifestListLoc);
  }

  private static StorageUri metadataStorageUri(ContentReference contentReference) {
    String metadataLocation =
        Preconditions.checkNotNull(
            contentReference.metadataLocation(),
            "Iceberg content is expected to have a non-null metadata-location for content-key %s on commit %s",
            contentReference.contentKey(),
            contentReference.commitId());
    StorageUri metadataLoc = checkUri("metadata", metadataLocation, contentReference);
    return metadataLoc;
  }

  static StorageUri baseUri(
      @Nonnull TableMetadata tableMetadata, @Nonnull ContentReference contentReference) {
    return baseUri(tableMetadata.location(), contentReference);
  }

  static StorageUri baseUri(
      @Nonnull ViewMetadata viewMetadata, @Nonnull ContentReference contentReference) {
    return baseUri(viewMetadata.location(), contentReference);
  }

  static StorageUri baseUri(@Nonnull String location, @Nonnull ContentReference contentReference) {
    String loc = location.endsWith("/") ? location : (location + "/");
    return checkUri("location", loc, contentReference);
  }

  static StorageUri manifestFileUri(
      @Nonnull ManifestFile mf, @Nonnull ContentReference contentReference) {
    String manifestFilePath =
        Preconditions.checkNotNull(
            mf.path(),
            "Iceberg manifest file is expected to have a non-null path for content-key %s on commit %s",
            contentReference.contentKey(),
            contentReference.commitId());
    return checkUri("manifest file", manifestFilePath, contentReference);
  }

  static StorageUri dataFileUri(
      @Nonnull String dataFilePath, @Nonnull ContentReference contentReference) {
    return checkUri("data file", dataFilePath, contentReference);
  }
}
