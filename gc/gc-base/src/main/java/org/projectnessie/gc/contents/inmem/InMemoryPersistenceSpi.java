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
package org.projectnessie.gc.contents.inmem;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Collections.emptySet;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.ImmutableLiveContentSet;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSet.Status;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.storage.uri.StorageUri;

/**
 * A <em>non-production</em> {@link PersistenceSpi} implementation that keeps all information in
 * memory.
 */
public class InMemoryPersistenceSpi implements PersistenceSpi {

  private final Map<UUID, InMemoryLiveContentSet> liveContentSets = new ConcurrentHashMap<>();

  static final class InMemoryLiveContentSet {

    /** Map of content-ID to set of content-references. */
    final Map<String, Set<ContentReference>> contents = new ConcurrentHashMap<>();

    final Map<String, Collection<StorageUri>> baseLocations = new ConcurrentHashMap<>();

    final AtomicReference<LiveContentSet> liveContentSet;

    final Set<FileReference> fileDeletions = new HashSet<>();

    InMemoryLiveContentSet(LiveContentSet liveContentSet) {
      this.liveContentSet = new AtomicReference<>(liveContentSet);
    }
  }

  @Override
  public long addIdentifiedLiveContent(
      @NotNull UUID liveSetId, @NotNull Stream<ContentReference> contentReference) {
    return contentReference
        .mapToLong(
            ref ->
                get(liveSetId)
                        .contents
                        .computeIfAbsent(
                            ref.contentId(), x -> Collections.synchronizedSet(new HashSet<>()))
                        .add(ref)
                    ? 1L
                    : 0L)
        .sum();
  }

  @Override
  public void startIdentifyLiveContents(@NotNull UUID liveSetId, @NotNull Instant created) {
    Preconditions.checkState(
        liveContentSets.putIfAbsent(
                liveSetId,
                new InMemoryLiveContentSet(
                    LiveContentSet.builder()
                        .persistenceSpi(this)
                        .id(liveSetId)
                        .created(created)
                        .status(Status.IDENTIFY_IN_PROGRESS)
                        .build()))
            == null,
        "Duplicate liveSetId " + liveSetId);
  }

  @Override
  public void finishedIdentifyLiveContents(
      @NotNull UUID liveSetId, @NotNull Instant finished, @Nullable Throwable failure) {
    get(liveSetId)
        .liveContentSet
        .getAndUpdate(
            current -> {
              ImmutableLiveContentSet.Builder b =
                  assertStatus(current, Status.IDENTIFY_IN_PROGRESS)
                      .unbuild()
                      .identifyCompleted(finished);
              if (failure != null) {
                b.status(Status.IDENTIFY_FAILED).errorMessage(failure.toString());
              } else {
                b.status(Status.IDENTIFY_SUCCESS);
              }
              return b.build();
            });
  }

  @Override
  public LiveContentSet startExpireContents(@NotNull UUID liveSetId, @NotNull Instant started) {
    return get(liveSetId)
        .liveContentSet
        .getAndUpdate(
            current ->
                assertStatus(current, Status.IDENTIFY_SUCCESS)
                    .unbuild()
                    .expiryStarted(started)
                    .status(Status.EXPIRY_IN_PROGRESS)
                    .build());
  }

  @Override
  public LiveContentSet finishedExpireContents(
      @NotNull UUID liveSetId, @NotNull Instant finished, @Nullable Throwable failure) {
    return get(liveSetId)
        .liveContentSet
        .getAndUpdate(
            current -> {
              ImmutableLiveContentSet.Builder b =
                  assertStatus(current, Status.EXPIRY_IN_PROGRESS)
                      .unbuild()
                      .expiryCompleted(finished);
              if (failure != null) {
                b.status(Status.EXPIRY_FAILED).errorMessage(getStackTraceAsString(failure));
              } else {
                b.status(Status.EXPIRY_SUCCESS);
              }
              return b.build();
            });
  }

  @Override
  public LiveContentSet getLiveContentSet(@NotNull UUID liveSetId)
      throws LiveContentSetNotFoundException {
    try {
      return get(liveSetId).liveContentSet.get();
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof LiveContentSetNotFoundException) {
        throw (LiveContentSetNotFoundException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public long fetchDistinctContentIdCount(@NotNull UUID liveSetId) {
    return getOptional(liveSetId).map(lcs -> (long) lcs.contents.size()).orElse(0L);
  }

  @Override
  public Stream<String> fetchContentIds(@NotNull UUID liveSetId) {
    return getOptional(liveSetId).map(lcs -> lcs.contents.keySet().stream()).orElse(Stream.empty());
  }

  @Override
  public Stream<ContentReference> fetchContentReferences(
      @NotNull UUID liveSetId, @NotNull String contentId) {
    return getOptional(liveSetId)
        .map(lcs -> lcs.contents.getOrDefault(contentId, emptySet()).stream())
        .orElse(Stream.empty());
  }

  @Override
  public void associateBaseLocations(
      UUID liveSetId, String contentId, Collection<StorageUri> baseLocations) {
    assertStatus(get(liveSetId), Status.EXPIRY_IN_PROGRESS)
        .baseLocations
        .computeIfAbsent(contentId, x -> new HashSet<>())
        .addAll(baseLocations);
  }

  @Override
  public Stream<StorageUri> fetchBaseLocations(UUID liveSetId, String contentId) {
    return get(liveSetId).baseLocations.getOrDefault(contentId, emptySet()).stream();
  }

  @Override
  public Stream<StorageUri> fetchAllBaseLocations(UUID liveSetId) {
    return liveContentSets.values().stream()
        .flatMap(lcs -> lcs.baseLocations.values().stream())
        .flatMap(Collection::stream);
  }

  private InMemoryLiveContentSet get(UUID liveSetId) {
    InMemoryLiveContentSet lcs = liveContentSets.get(liveSetId);
    if (lcs == null) {
      throw new IllegalStateException(new LiveContentSetNotFoundException(liveSetId));
    }
    return lcs;
  }

  private Optional<InMemoryLiveContentSet> getOptional(UUID liveSetId) {
    return Optional.ofNullable(liveContentSets.get(liveSetId));
  }

  @Override
  public void deleteLiveContentSet(UUID liveSetId) {
    Preconditions.checkState(
        liveContentSets.remove(liveSetId) != null, "Live content set not found %s", liveSetId);
  }

  @Override
  public Stream<LiveContentSet> getAllLiveContents() {
    return liveContentSets.values().stream().map(mock -> mock.liveContentSet.get());
  }

  @Override
  public long addFileDeletions(UUID liveSetId, Stream<FileReference> files) {
    InMemoryLiveContentSet lcs = assertStatus(get(liveSetId), Status.EXPIRY_IN_PROGRESS);
    synchronized (lcs.fileDeletions) {
      long count = 0L;
      for (Iterator<FileReference> iter = files.iterator(); iter.hasNext(); ) {
        if (lcs.fileDeletions.add(iter.next())) {
          count++;
        }
      }
      return count;
    }
  }

  @Override
  public Stream<FileReference> fetchFileDeletions(UUID liveSetId) {
    InMemoryLiveContentSet lcs = assertStatus(get(liveSetId), Status.EXPIRY_SUCCESS);
    return lcs.fileDeletions.stream()
        .sorted(Comparator.comparing(FileReference::base).thenComparing(FileReference::path));
  }

  private static InMemoryLiveContentSet assertStatus(InMemoryLiveContentSet current, Status valid) {
    assertStatus(current.liveContentSet.get(), valid);
    return current;
  }

  private static LiveContentSet assertStatus(LiveContentSet current, Status valid) {
    Preconditions.checkState(
        current.status() == valid,
        "Expected current status of " + valid + ", but is " + current.status());
    return current;
  }
}
