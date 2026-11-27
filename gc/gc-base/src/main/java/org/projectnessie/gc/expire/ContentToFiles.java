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
package org.projectnessie.gc.expire;

import com.google.errorprone.annotations.MustBeClosed;
import jakarta.validation.constraints.NotNull;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.model.Content;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Retrieve all files/objects for a specific {@link ContentReference}, specific to table formats.
 */
@FunctionalInterface
public interface ContentToFiles {

  /**
   * Extracts all files and base locations from the given {@link Content} object.
   *
   * @param contentReference content object to extract all files and base location(s) from
   * @return stream of all files used by the provided content object - only the {@link
   *     FileReference#base()} and {@link FileReference#path()} attributes of the returned {@link
   *     FileReference}s contain useful information (aka the modification time is set to an
   *     arbitrary value and must be ignored). The {@link FileReference#base()} attribute
   *     <em>must</em> correspond to the base path of the referenced content.
   */
  @MustBeClosed
  Stream<FileReference> extractFiles(@NotNull ContentReference contentReference);

  /**
   * Extracts the base location(s) of the given {@link Content} object, the same as the {@link
   * FileReference#base() base locations} of the files returned by {@link
   * #extractFiles(ContentReference)}. Used to identify base locations that are shared by multiple
   * contents, before any file is deleted.
   *
   * <p>The default implementation extracts all files, implementations should provide a cheaper one.
   */
  @MustBeClosed
  default Stream<StorageUri> extractBaseLocations(@NotNull ContentReference contentReference) {
    @SuppressWarnings("MustBeClosedChecker")
    Stream<FileReference> files = extractFiles(contentReference);
    return files.map(FileReference::base).distinct();
  }
}
