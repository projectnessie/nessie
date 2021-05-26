/*
 * Copyright (C) 2020 Dremio
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
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.FileIO;

/**
 * Utility to get at the manifest files to retrieve data file list. Not publicly exposed by Iceberg.
 */
public final class DataFileCollector {

  private DataFileCollector() {}

  /** retrieve all data files from a set of manifest files. */
  public static Stream<String> dataFiles(FileIO io, List<ManifestFile> allManifests) {
    return allManifests.stream()
        .flatMap(
            manifest -> {
              try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
                return StreamSupport.stream(reader.entries().spliterator(), false)
                    .map(entry -> entry.file().path().toString());
              } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Failed to read manifest file: %s", manifest.path()), e);
              }
            });
  }
}
