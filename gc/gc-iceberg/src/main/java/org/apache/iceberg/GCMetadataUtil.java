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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.gc.iceberg.ExpireContentsProcedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class with same package as Iceberg because {@link ManifestFiles#open(ManifestFile, FileIO)}
 * is package private.
 */
public final class GCMetadataUtil {

  private static final Logger LOG = LoggerFactory.getLogger(GCMetadataUtil.class);

  public static List<String> computeAllFiles(Snapshot snapshot, FileIO io) {
    // New ManifestList file created after compaction will have new manifest files
    // pointing to older data files with a marking (status) that it is deleted data files.
    // So, skip those datafiles.
    Predicate<ManifestEntry<?>> activeDataFilesPredicate =
        entry -> entry.status() != ManifestEntry.Status.DELETED;

    try {
      Stream<String> dataFileStream =
          snapshot.allManifests().stream()
              .flatMap(
                  manifest -> {
                    try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
                      return StreamSupport.stream(reader.entries().spliterator(), false)
                          .filter(activeDataFilesPredicate)
                          .map(
                              entry ->
                                  ExpireContentsProcedure.FileType.DATA_FILE.name()
                                      + "#"
                                      + entry.file().path().toString());
                    } catch (IOException | UncheckedIOException e) {
                      LOG.warn(
                          "Failed to read the manifest file. Might have deleted in the previous GC run."
                              + " Hence, Ignoring the error.",
                          e);
                      return null;
                    }
                  })
              .filter(Objects::nonNull);

      Stream<String> manifestListFile =
          Stream.of(
              ExpireContentsProcedure.FileType.ICEBERG_MANIFESTLIST.name()
                  + "#"
                  + snapshot.manifestListLocation());

      Stream<String> manifestStream =
          snapshot.allManifests().stream()
              .map(
                  manifest ->
                      ExpireContentsProcedure.FileType.ICEBERG_MANIFEST.name()
                          + "#"
                          + manifest.path());

      return Stream.concat(manifestListFile, Stream.concat(manifestStream, dataFileStream))
          .collect(Collectors.toList());
    } catch (NotFoundException e) {
      // There is no checkpoint implementation in gc. So, current gc output can contain the expired
      // contents that are cleaned in the previous run.
      // The manifestList, manifest and data files of those contents might have
      // deleted in the previous run and can lead to NotFoundException in the current run.
      LOG.warn(
          "Failed to read the manifestList file. Might have deleted in the previous GC run."
              + " Hence, Ignoring the error.",
          e);
    }
    return Collections.emptyList();
  }
}
