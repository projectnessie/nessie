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
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.files.FileReference;

/**
 * Interface to be implemented by Nessie GC persistence implementations. <em>Only</em> to be used by
 * {@link LiveContentSetsRepository} and {@link LiveContentSet}, other code must interact with the
 * {@link LiveContentSetsRepository} or {@link LiveContentSet}, but <em>never</em> this interface.
 */
public interface PersistenceSpi {

  void deleteLiveContentSet(UUID liveSetId);

  @MustBeClosed
  Stream<LiveContentSet> getAllLiveContents();

  void startIdentifyLiveContents(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull Instant created);

  long addIdentifiedLiveContent(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull Stream<ContentReference> contentReference);

  void finishedIdentifyLiveContents(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull Instant finished,
      @Nullable Throwable failure);

  LiveContentSet startExpireContents(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull Instant started);

  LiveContentSet finishedExpireContents(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull Instant finished,
      @Nullable Throwable failure);

  LiveContentSet getLiveContentSet(@NotNull @jakarta.validation.constraints.NotNull UUID liveSetId)
      throws LiveContentSetNotFoundException;

  long fetchDistinctContentIdCount(@NotNull @jakarta.validation.constraints.NotNull UUID liveSetId);

  @MustBeClosed
  Stream<String> fetchContentIds(@NotNull @jakarta.validation.constraints.NotNull UUID liveSetId);

  Stream<ContentReference> fetchContentReferences(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull String contentId);

  void associateBaseLocations(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull String contentId,
      @NotNull @jakarta.validation.constraints.NotNull Collection<URI> baseLocations);

  @MustBeClosed
  Stream<URI> fetchBaseLocations(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull String contentId);

  @MustBeClosed
  Stream<URI> fetchAllBaseLocations(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId);

  /**
   * Records the given files to be later returned by {@link #fetchFileDeletions(UUID)}, ignores
   * duplicates.
   *
   * @return the number of actually added files
   */
  long addFileDeletions(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId,
      @NotNull @jakarta.validation.constraints.NotNull Stream<FileReference> files);

  /**
   * Returns the {@link #addFileDeletions(UUID, Stream)} recorded file deletions (aka deferred
   * deletes) grouped by base path.
   */
  @MustBeClosed
  Stream<FileReference> fetchFileDeletions(
      @NotNull @jakarta.validation.constraints.NotNull UUID liveSetId);
}
