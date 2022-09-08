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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.expire.ContentToFiles;
import org.projectnessie.gc.files.FileReference;

/**
 * Provides functionality to extract information about files and base locations from Iceberg
 * content, which is the one Iceberg table snapshot referred to by a Nessie commit.
 */
@Value.Immutable
public abstract class IcebergContentToFiles implements ContentToFiles {

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

    TableMetadata tableMetadata = TableMetadataParser.read(io, contentReference.metadataLocation());

    long snapshotId = contentReference.snapshotId();
    URI metadataUri = URI.create(contentReference.metadataLocation());

    // This is to respect Nessie's global state
    Snapshot snapshot =
        snapshotId < 0L ? tableMetadata.currentSnapshot() : tableMetadata.snapshot(snapshotId);

    Stream<URI> allFiles;

    if (snapshot != null) {
      allFiles = Stream.of(metadataUri, URI.create(snapshot.manifestListLocation()));

      allFiles =
          Stream.concat(
              allFiles,
              Stream.of("")
                  // .flatMap() for lazy loading
                  .flatMap(
                      x -> {
                        @SuppressWarnings("MustBeClosedChecker")
                        Stream<URI> r = allManifestsAndDataFiles(io, snapshot);
                        return r;
                      }));
    } else {
      // snapshot == null
      allFiles = Stream.of(metadataUri);
    }

    URI baseUri = baseUri(tableMetadata);

    return allFiles.map(baseUri::relativize).map(u -> FileReference.of(u, baseUri, -1L));
  }

  protected URI baseUri(TableMetadata tableMetadata) {
    String location = tableMetadata.location();
    return URI.create(location.endsWith("/") ? location : (location + "/"));
  }

  /**
   * For the given {@link Snapshot}, provide a {@link Stream} of all manifest files with {@link
   * #allDataFiles(FileIO, ManifestFile) all included data files}.
   */
  @MustBeClosed
  protected Stream<URI> allManifestsAndDataFiles(FileIO io, Snapshot snapshot) {
    return allManifests(io, snapshot)
        .flatMap(
            mf -> {
              @SuppressWarnings("MustBeClosedChecker")
              Stream<URI> allDataFile = allDataFiles(io, mf);
              return Stream.concat(Stream.of(URI.create(mf.path())), allDataFile);
            });
  }

  /** Provide all {@link ManifestFile}s for the given {@link Snapshot}. */
  protected Stream<ManifestFile> allManifests(FileIO io, Snapshot snapshot) {
    return snapshot.allManifests(io).stream();
  }

  /**
   * For the given {@link ManifestFile}, provide a {@link Stream} of <em>all</em> data files, means
   * including all {@link org.apache.iceberg.ManifestEntry}s of every status ({@code EXISTING},
   * {@code ADDED}, {@code DELETED}.
   */
  @MustBeClosed
  protected Stream<URI> allDataFiles(FileIO io, ManifestFile manifestFile) {
    CloseableIterable<String> iter = ManifestFiles.readPaths(manifestFile, io);
    return StreamSupport.stream(iter.spliterator(), false)
        .map(URI::create)
        .onClose(
            () -> {
              try {
                iter.close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }
}
