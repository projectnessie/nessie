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

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReaderUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.expire.ContentToFiles;
import org.projectnessie.gc.files.FileReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality to extract information about files and base locations from Iceberg
 * content, which is the one Iceberg table snapshot referred to by a Nessie commit.
 */
@Value.Immutable
public abstract class IcebergContentToFiles implements ContentToFiles {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergContentToFiles.class);
  public static final String S3_KEY_NOT_FOUND =
      "software.amazon.awssdk.services.s3.model.NoSuchKeyException";

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
    FileIO io = io();

    TableMetadata tableMetadata;
    try {
      tableMetadata = TableMetadataParser.read(io, contentReference.metadataLocation());
    } catch (Exception notFoundCandidate) {
      if (notFoundCandidate instanceof NotFoundException
          // Iceberg does not map software.amazon.awssdk.services.s3.model.NoSuchKeyException to
          // its native org.apache.iceberg.exceptions.NotFoundException,
          || S3_KEY_NOT_FOUND.equals(notFoundCandidate.getClass().getName())) {
        // It is safe to assume that a missing table-metadata means no referenced files.
        // A table-metadata can be missing, because a previous Nessie GC "sweep" phase deleted it.
        LOGGER.info(
            "Table metadata {} for snapshot ID {} for content-key {} at Nessie commit {} does not exist, probably already deleted, assuming no files",
            contentReference.metadataLocation(),
            contentReference.snapshotId(),
            contentReference.contentKey(),
            contentReference.commitId());
        return Stream.empty();
      }
      throw new RuntimeException(notFoundCandidate);
    }

    long snapshotId =
        Objects.requireNonNull(
            contentReference.snapshotId(),
            "Iceberg content is expected to have a non-null snapshot-ID");

    Snapshot snapshot =
        snapshotId < 0L ? tableMetadata.currentSnapshot() : tableMetadata.snapshot(snapshotId);

    Stream<URI> allFiles = elementaryUrisFromSnapshot(snapshot, contentReference);

    if (snapshot != null) {
      allFiles =
          Stream.concat(
              allFiles,
              Stream.of("")
                  // .flatMap() for lazy loading
                  .flatMap(
                      x -> {
                        @SuppressWarnings("MustBeClosedChecker")
                        Stream<URI> r = allManifestsAndDataFiles(io, snapshot, contentReference);
                        return r;
                      }));
    }

    URI baseUri = baseUri(tableMetadata, contentReference);

    return allFiles.map(baseUri::relativize).map(u -> FileReference.of(u, baseUri, -1L));
  }

  /**
   * For the given {@link Snapshot}, provide a {@link Stream} of all manifest files with {@link
   * #allDataAndDeleteFiles(FileIO, ManifestFile, ContentReference) all included data and delete
   * files}.
   */
  @MustBeClosed
  static Stream<URI> allManifestsAndDataFiles(
      FileIO io, Snapshot snapshot, ContentReference contentReference) {
    return allManifests(io, snapshot)
        .flatMap(
            mf -> {
              URI manifestFileUri = manifestFileUri(mf, contentReference);
              @SuppressWarnings("MustBeClosedChecker")
              Stream<URI> allDataAndDeleteFiles = allDataAndDeleteFiles(io, mf, contentReference);
              return Stream.concat(Stream.of(manifestFileUri), allDataAndDeleteFiles);
            });
  }

  /** Provide all {@link ManifestFile}s for the given {@link Snapshot}. */
  static Stream<ManifestFile> allManifests(FileIO io, Snapshot snapshot) {
    return snapshot.allManifests(io).stream();
  }

  /**
   * For the given {@link ManifestFile}, provide a {@link Stream} of <em>all</em> data and delete
   * files, means including all {@link org.apache.iceberg.ManifestEntry}s of every status ({@code
   * EXISTING}, {@code ADDED}, {@code DELETED}.
   */
  @MustBeClosed
  static Stream<URI> allDataAndDeleteFiles(
      FileIO io, ManifestFile manifestFile, ContentReference contentReference) {
    CloseableIterable<String> iter = ManifestReaderUtil.readPathsFromManifest(manifestFile, io);
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
   * All processed {@link URI}s must have a schema part and, if the schema is {@code file}, the path
   * must be an absolute path.
   */
  static URI checkUri(String type, URI uri, ContentReference contentReference) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      String path = uri.getPath();
      Preconditions.checkArgument(
          path.startsWith("/"),
          "Iceberg content reference points to the %s URI '%s' as content-key %s on commit %s without a scheme and with a relative path, which is not supported.",
          type,
          uri,
          contentReference.contentKey(),
          contentReference.commitId());
      uri = URI.create("file://" + path);
    }
    if ("file".equals(scheme)) {
      String schemeSpecific = uri.getSchemeSpecificPart();
      Preconditions.checkArgument(
          schemeSpecific.startsWith("/"),
          "Iceberg content reference points to the %s URI '%s' as content-key %s on commit %s with a non-absolute scheme-specific-part %s, which is not supported.",
          type,
          uri,
          contentReference.contentKey(),
          contentReference.commitId(),
          schemeSpecific);
      Preconditions.checkArgument(
          uri.getHost() == null,
          "Iceberg content reference points to the host-specific %s URI '%s' as content-key %s on commit %s without a scheme, which is not supported.",
          type,
          uri,
          contentReference.contentKey(),
          contentReference.commitId());
      if (!schemeSpecific.startsWith("///")) {
        uri = URI.create("file://" + uri.getPath());
      }
    }
    return uri;
  }

  static Stream<URI> elementaryUrisFromSnapshot(
      Snapshot snapshot, ContentReference contentReference) {
    String metadataLocation =
        Preconditions.checkNotNull(
            contentReference.metadataLocation(),
            "Iceberg content is expected to have a non-null metadata-location for content-key %s on commit %s",
            contentReference.contentKey(),
            contentReference.commitId());
    URI metadataUri = URI.create(metadataLocation);
    metadataUri = checkUri("metadata", metadataUri, contentReference);

    if (snapshot == null) {
      return Stream.of(metadataUri);
    }

    String manifestListLocation = snapshot.manifestListLocation();
    if (manifestListLocation == null) {
      // Iceberg spec v1 has the manifest files embedded in the table-metadata, Iceberg spec v2
      // has a separate manifest list file.
      return Stream.of(metadataUri);
    }

    URI manifestListUri = URI.create(snapshot.manifestListLocation());
    manifestListUri = checkUri("manifest list", manifestListUri, contentReference);

    return Stream.of(metadataUri, manifestListUri);
  }

  static URI baseUri(
      @Nonnull TableMetadata tableMetadata, @Nonnull ContentReference contentReference) {
    String location = tableMetadata.location();
    URI uri = URI.create(location.endsWith("/") ? location : (location + "/"));
    return checkUri("location", uri, contentReference);
  }

  static URI manifestFileUri(@Nonnull ManifestFile mf, @Nonnull ContentReference contentReference) {
    String manifestFilePath =
        Preconditions.checkNotNull(
            mf.path(),
            "Iceberg manifest file is expected to have a non-null path for content-key %s on commit %s",
            contentReference.contentKey(),
            contentReference.commitId());
    URI manifestFile = URI.create(manifestFilePath);
    return checkUri("manifest file", manifestFile, contentReference);
  }

  static URI dataFileUri(@Nonnull String dataFilePath, @Nonnull ContentReference contentReference) {
    URI uri = URI.create(dataFilePath);
    return checkUri("data file", uri, contentReference);
  }
}
