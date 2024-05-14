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
package org.projectnessie.gc.contents.spi;

import com.google.errorprone.annotations.MustBeClosed;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Interface to be implemented by Nessie GC persistence implementations. <em>Only</em> to be used by
 * {@link LiveContentSetsRepository} and {@link LiveContentSet}, other code must interact with the
 * {@link LiveContentSetsRepository} or {@link LiveContentSet}, but <em>never</em> this interface.
 */
public interface PersistenceSpi {

  void deleteLiveContentSet(UUID liveSetId);

  @MustBeClosed
  Stream<LiveContentSet> getAllLiveContents();

  void startIdentifyLiveContents(@NotNull UUID liveSetId, @NotNull Instant created);

  long addIdentifiedLiveContent(
      @NotNull UUID liveSetId, @NotNull Stream<ContentReference> contentReference);

  void finishedIdentifyLiveContents(
      @NotNull UUID liveSetId, @NotNull Instant finished, @Nullable Throwable failure);

  LiveContentSet startExpireContents(@NotNull UUID liveSetId, @NotNull Instant started);

  LiveContentSet finishedExpireContents(
      @NotNull UUID liveSetId, @NotNull Instant finished, @Nullable Throwable failure);

  LiveContentSet getLiveContentSet(@NotNull UUID liveSetId) throws LiveContentSetNotFoundException;

  long fetchDistinctContentIdCount(@NotNull UUID liveSetId);

  @MustBeClosed
  Stream<String> fetchContentIds(@NotNull UUID liveSetId);

  Stream<ContentReference> fetchContentReferences(
      @NotNull UUID liveSetId, @NotNull String contentId);

  void associateBaseLocations(
      @NotNull UUID liveSetId,
      @NotNull String contentId,
      @NotNull Collection<StorageUri> baseLocations);

  @MustBeClosed
  Stream<StorageUri> fetchBaseLocations(@NotNull UUID liveSetId, @NotNull String contentId);

  @MustBeClosed
  Stream<StorageUri> fetchAllBaseLocations(@NotNull UUID liveSetId);

  /**
   * Records the given files to be later returned by {@link #fetchFileDeletions(UUID)}, ignores
   * duplicates.
   *
   * @return the number of actually added files
   */
  long addFileDeletions(@NotNull UUID liveSetId, @NotNull Stream<FileReference> files);

  /**
   * Returns the {@link #addFileDeletions(UUID, Stream)} recorded file deletions (aka deferred
   * deletes) grouped by base path.
   */
  @MustBeClosed
  Stream<FileReference> fetchFileDeletions(@NotNull UUID liveSetId);
}
