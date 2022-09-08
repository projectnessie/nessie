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
package org.projectnessie.gc.iceberg.inttest;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.time.Instant;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.ImmutableLiveContentSet;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSet.Status;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.files.FileReference;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/**
 * Provides a specialized implementation of {@link PersistenceSpi} for {@link TestManyObjects} to
 * track the content {@link ContentReference}s for Nessie GC's mark-and-sweep approach using a
 * minimal amount of memory via {@link ContentInfo}.
 */
final class HugePersistenceSpi implements PersistenceSpi {

  private final HugeRepositoryConnector repositoryConnector;

  private UUID id;
  private Status status;
  private final ImmutableLiveContentSet.Builder liveContentSet =
      LiveContentSet.builder().persistenceSpi(this);

  static final class ContentInfo {

    final Set<URI> baseLocations = new HashSet<>();
    BitSet liveSnapshotIds = new BitSet();

    void liveSnapshotId(int snapshotId) {
      liveSnapshotIds.set(snapshotId);
    }
  }

  private final Map<String, ContentInfo> contentInfo = new ConcurrentHashMap<>();

  public HugePersistenceSpi(HugeRepositoryConnector repositoryConnector) {
    this.repositoryConnector = repositoryConnector;
  }

  @Override
  public void startIdentifyLiveContents(UUID liveSetId, Instant created) {
    Preconditions.checkArgument(id == null);
    this.id = liveSetId;
    this.status = Status.IDENTIFY_IN_PROGRESS;
    liveContentSet.status(status).created(created).id(liveSetId);
  }

  @Override
  public long addIdentifiedLiveContent(UUID liveSetId, Stream<ContentReference> contentReference) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    return contentReference
        .peek(
            ref ->
                contentInfo
                    .computeIfAbsent(ref.contentId(), c -> new ContentInfo())
                    .liveSnapshotId(ref.snapshotId().intValue()))
        .count();
  }

  @Override
  public void finishedIdentifyLiveContents(
      UUID liveSetId, Instant finished, @Nullable Throwable failure) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id) && failure == null);
    Preconditions.checkState(status == Status.IDENTIFY_IN_PROGRESS);
    status = Status.IDENTIFY_SUCCESS;
    liveContentSet.status(status).identifyCompleted(finished);
  }

  @Override
  public LiveContentSet startExpireContents(UUID liveSetId, Instant started) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    Preconditions.checkState(status == Status.IDENTIFY_SUCCESS);
    status = Status.EXPIRY_IN_PROGRESS;
    liveContentSet.status(status).expiryStarted(started);
    return liveContentSet.build();
  }

  @Override
  public LiveContentSet finishedExpireContents(
      UUID liveSetId, Instant finished, @Nullable Throwable failure) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id) && failure == null);
    Preconditions.checkState(status == Status.EXPIRY_IN_PROGRESS);
    status = Status.EXPIRY_SUCCESS;
    liveContentSet.status(status).expiryCompleted(finished);
    return liveContentSet.build();
  }

  @Override
  public LiveContentSet getLiveContentSet(UUID liveSetId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    return liveContentSet.build();
  }

  @Override
  public long fetchDistinctContentIdCount(UUID liveSetId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    return contentInfo.size();
  }

  @Override
  public Stream<String> fetchContentIds(UUID liveSetId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    return contentInfo.keySet().stream();
  }

  @Override
  public Stream<ContentReference> fetchContentReferences(UUID liveSetId, String contentId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    ContentInfo info = contentInfo.get(contentId);
    if (info == null) {
      return Stream.empty();
    }
    return info.liveSnapshotIds.stream()
        .mapToObj(
            snapshotId ->
                ContentReference.icebergTable(
                    contentId,
                    "deadbeeffeed",
                    HugeRepositoryConnector.contentKey(contentId),
                    repositoryConnector.metadataLocation(contentId, snapshotId),
                    snapshotId));
  }

  @Override
  public void associateBaseLocations(
      UUID liveSetId, String contentId, Collection<URI> baseLocations) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    contentInfo
        .computeIfAbsent(contentId, c -> new ContentInfo())
        .baseLocations
        .addAll(baseLocations);
  }

  @Override
  public Stream<URI> fetchBaseLocations(UUID liveSetId, String contentId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    ContentInfo info = contentInfo.get(contentId);
    return info != null ? info.baseLocations.stream() : Stream.empty();
  }

  @Override
  public Stream<URI> fetchAllBaseLocations(UUID liveSetId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    return contentInfo.values().stream().flatMap(i -> i.baseLocations.stream());
  }

  @Override
  public void deleteLiveContentSet(UUID liveSetId) {
    Preconditions.checkArgument(liveSetId != null && liveSetId.equals(id));
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<LiveContentSet> getAllLiveContents() {
    if (id == null) {
      return Stream.empty();
    }
    return Stream.of(liveContentSet.build());
  }

  @Override
  public long addFileDeletions(UUID liveSetId, Stream<FileReference> files) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<FileReference> fetchFileDeletions(UUID liveSetId) {
    throw new UnsupportedOperationException();
  }
}
