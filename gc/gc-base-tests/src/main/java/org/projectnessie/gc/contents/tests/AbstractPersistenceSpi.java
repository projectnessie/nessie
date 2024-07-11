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
package org.projectnessie.gc.contents.tests;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.model.ContentKey;
import org.projectnessie.storage.uri.StorageUri;

/** Tests for all {@link PersistenceSpi} implementations. */
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractPersistenceSpi {
  protected PersistenceSpi persistenceSpi;

  @InjectSoftAssertions protected SoftAssertions soft;

  @BeforeEach
  void newPersistenceSpi() {
    persistenceSpi = createPersistenceSpi();
  }

  protected abstract PersistenceSpi createPersistenceSpi();

  final class LiveSetVals {
    // Value ID doesn't matter
    final UUID id = UUID.randomUUID();
    final Instant identifyStart = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    final Instant identifyStart2 = identifyStart.plus(1, ChronoUnit.MINUTES);
    final Instant identifyFinish = identifyStart2.plus(1, ChronoUnit.MINUTES);
    final Instant identifyFinish2 = identifyFinish.plus(1, ChronoUnit.MINUTES);
    final Instant expiryStart = identifyFinish2.plus(1, ChronoUnit.MINUTES);
    final Instant expiryStart2 = expiryStart.plus(1, ChronoUnit.MINUTES);
    final Instant expiryFinish = expiryStart2.plus(1, ChronoUnit.MINUTES);
    final Instant expiryFinish2 = expiryFinish.plus(1, ChronoUnit.MINUTES);

    final List<ContentReference> refs;

    LiveSetVals() {
      this(5);
    }

    LiveSetVals(int numRefs) {
      refs =
          IntStream.range(0, numRefs)
              .mapToObj(
                  i ->
                      icebergContent(
                          (i & 1) == 1 ? ICEBERG_VIEW : ICEBERG_TABLE,
                          "cid-" + i,
                          "123456780" + i,
                          ContentKey.of("a", "b-" + id, "c-" + i),
                          "meta-" + id + i,
                          i))
              .collect(Collectors.toList());
    }

    Set<String> contentIds() {
      return refs.stream().map(ContentReference::contentId).collect(Collectors.toSet());
    }

    List<ContentReference> refsForCid(String contentId) {
      return refs.stream()
          .filter(r -> r.contentId().equals(contentId))
          .collect(Collectors.toList());
    }

    void startIdentify() throws LiveContentSetNotFoundException {
      persistenceSpi.startIdentifyLiveContents(id, identifyStart);
      LiveContentSet l = persistenceSpi.getLiveContentSet(id);
      soft.assertThat(l)
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.IDENTIFY_IN_PROGRESS),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isNull(),
              lcs -> assertThat(lcs.expiryStarted()).isNull(),
              lcs -> assertThat(lcs.expiryCompleted()).isNull());

      soft.assertThatThrownBy(() -> persistenceSpi.startIdentifyLiveContents(id, identifyStart2))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Duplicate");

      soft.assertThatThrownBy(() -> persistenceSpi.startExpireContents(id, expiryStart2))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of IDENTIFY_SUCCESS, but is IDENTIFY_IN_PROGRESS");
      soft.assertThatThrownBy(() -> persistenceSpi.finishedExpireContents(id, expiryFinish, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of EXPIRY_IN_PROGRESS, but is IDENTIFY_IN_PROGRESS");

      soft.assertAll();
    }

    void finishIdentify() throws LiveContentSetNotFoundException {

      persistenceSpi.finishedIdentifyLiveContents(id, identifyFinish, null);
      LiveContentSet l = persistenceSpi.getLiveContentSet(id);
      soft.assertThat(l)
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.IDENTIFY_SUCCESS),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isEqualTo(identifyFinish),
              lcs -> assertThat(lcs.expiryStarted()).isNull(),
              lcs -> assertThat(lcs.expiryCompleted()).isNull(),
              lcs -> assertThat(lcs.errorMessage()).isNull());

      soft.assertThatThrownBy(
              () -> persistenceSpi.finishedIdentifyLiveContents(id, identifyFinish2, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of IDENTIFY_IN_PROGRESS, but is IDENTIFY_SUCCESS");
      soft.assertThatThrownBy(() -> persistenceSpi.finishedExpireContents(id, expiryFinish2, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of EXPIRY_IN_PROGRESS, but is IDENTIFY_SUCCESS");

      soft.assertAll();
    }

    void startExpiry() throws LiveContentSetNotFoundException {
      persistenceSpi.startExpireContents(id, expiryStart);

      soft.assertThat(persistenceSpi.getLiveContentSet(id))
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.EXPIRY_IN_PROGRESS),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isEqualTo(identifyFinish),
              lcs -> assertThat(lcs.expiryStarted()).isEqualTo(expiryStart),
              lcs -> assertThat(lcs.expiryCompleted()).isNull(),
              lcs -> assertThat(lcs.errorMessage()).isNull());

      soft.assertThatThrownBy(() -> persistenceSpi.startExpireContents(id, expiryStart2))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of IDENTIFY_SUCCESS, but is EXPIRY_IN_PROGRESS");
      soft.assertThatThrownBy(
              () -> persistenceSpi.finishedIdentifyLiveContents(id, identifyFinish2, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of IDENTIFY_IN_PROGRESS, but is EXPIRY_IN_PROGRESS");

      soft.assertThat(persistenceSpi.getLiveContentSet(id))
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.EXPIRY_IN_PROGRESS),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isEqualTo(identifyFinish),
              lcs -> assertThat(lcs.expiryStarted()).isEqualTo(expiryStart),
              lcs -> assertThat(lcs.expiryCompleted()).isNull(),
              lcs -> assertThat(lcs.errorMessage()).isNull());

      soft.assertAll();
    }

    void finishExpiry() throws LiveContentSetNotFoundException {
      persistenceSpi.finishedExpireContents(id, expiryFinish, null);

      soft.assertThatThrownBy(() -> persistenceSpi.startExpireContents(id, expiryStart2))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of IDENTIFY_SUCCESS, but is EXPIRY_SUCCESS");
      soft.assertThatThrownBy(
              () -> persistenceSpi.finishedIdentifyLiveContents(id, identifyFinish2, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of IDENTIFY_IN_PROGRESS, but is EXPIRY_SUCCESS");
      soft.assertThatThrownBy(() -> persistenceSpi.finishedExpireContents(id, expiryFinish2, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "Expected current status of EXPIRY_IN_PROGRESS, but is EXPIRY_SUCCESS");

      soft.assertThat(persistenceSpi.getLiveContentSet(id))
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.EXPIRY_SUCCESS),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isEqualTo(identifyFinish),
              lcs -> assertThat(lcs.expiryStarted()).isEqualTo(expiryStart),
              lcs -> assertThat(lcs.expiryCompleted()).isEqualTo(expiryFinish),
              lcs -> assertThat(lcs.errorMessage()).isNull());

      soft.assertAll();
    }

    void assertIdentifyFailure() throws LiveContentSetNotFoundException {
      LiveContentSet l = persistenceSpi.getLiveContentSet(id);
      soft.assertThat(l)
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.IDENTIFY_FAILED),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isEqualTo(identifyFinish),
              lcs -> assertThat(lcs.expiryStarted()).isNull(),
              lcs -> assertThat(lcs.expiryCompleted()).isNull(),
              lcs -> assertThat(lcs.errorMessage()).contains("identify-failed-reason-string"));
      try (Stream<LiveContentSet> all = persistenceSpi.getAllLiveContents()) {
        soft.assertThat(all).containsExactly(l);
      }

      soft.assertAll();
    }

    void assertExpiryFailure() throws LiveContentSetNotFoundException {
      LiveContentSet l = persistenceSpi.getLiveContentSet(id);
      soft.assertThat(l)
          .satisfies(
              lcs -> assertThat(lcs.status()).isSameAs(LiveContentSet.Status.EXPIRY_FAILED),
              lcs -> assertThat(lcs.created()).isEqualTo(identifyStart),
              lcs -> assertThat(lcs.identifyCompleted()).isEqualTo(identifyFinish),
              lcs -> assertThat(lcs.expiryStarted()).isEqualTo(expiryStart),
              lcs -> assertThat(lcs.expiryCompleted()).isEqualTo(expiryFinish),
              lcs -> assertThat(lcs.errorMessage()).contains("expire-failed-reason-string"));
      try (Stream<LiveContentSet> all = persistenceSpi.getAllLiveContents()) {
        soft.assertThat(all).containsExactly(l);
      }

      soft.assertAll();
    }

    void delete() throws Exception {
      LiveContentSet l = persistenceSpi.getLiveContentSet(id);
      l.delete();

      assertDeleted(id);

      try (Stream<LiveContentSet> list = persistenceSpi.getAllLiveContents()) {
        soft.assertThat(list).noneMatch(lcs -> lcs.id().equals(id));
      }
      soft.assertThatThrownBy(() -> persistenceSpi.getLiveContentSet(id))
          .isInstanceOf(LiveContentSetNotFoundException.class)
          .hasMessage("Live content set " + id + " not found");
      soft.assertThatThrownBy(l::delete)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Live content set not found " + l.id());

      soft.assertAll();
    }
  }

  @Test
  public void lifecycleAllOK() throws Exception {
    LiveSetVals vals = new LiveSetVals();

    vals.startIdentify();

    vals.finishIdentify();

    vals.startExpiry();

    vals.finishExpiry();

    vals.delete();
  }

  @Test
  public void lifecycleIdentifyFailure() throws Exception {
    LiveSetVals vals = new LiveSetVals();

    vals.startIdentify();

    persistenceSpi.finishedIdentifyLiveContents(
        vals.id, vals.identifyFinish, new Exception("identify-failed-reason-string"));

    vals.assertIdentifyFailure();

    soft.assertThatThrownBy(
            () -> persistenceSpi.finishedIdentifyLiveContents(vals.id, vals.identifyFinish2, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Expected current status of IDENTIFY_IN_PROGRESS, but is IDENTIFY_FAILED");
    soft.assertThatThrownBy(
            () -> persistenceSpi.finishedExpireContents(vals.id, vals.expiryFinish2, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Expected current status of EXPIRY_IN_PROGRESS, but is IDENTIFY_FAILED");
    soft.assertThatThrownBy(() -> persistenceSpi.startExpireContents(vals.id, vals.expiryStart2))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Expected current status of IDENTIFY_SUCCESS, but is IDENTIFY_FAILED");

    vals.assertIdentifyFailure();
  }

  @Test
  public void lifecycleExpiryFailure() throws Exception {
    LiveSetVals vals = new LiveSetVals();

    vals.startIdentify();

    vals.finishIdentify();

    vals.startExpiry();

    persistenceSpi.finishedExpireContents(
        vals.id, vals.expiryFinish, new Exception("expire-failed-reason-string"));

    vals.assertExpiryFailure();

    soft.assertThatThrownBy(() -> persistenceSpi.startExpireContents(vals.id, vals.expiryStart2))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected current status of IDENTIFY_SUCCESS, but is EXPIRY_FAILED");
    soft.assertThatThrownBy(
            () -> persistenceSpi.finishedIdentifyLiveContents(vals.id, vals.identifyFinish2, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Expected current status of IDENTIFY_IN_PROGRESS, but is EXPIRY_FAILED");
    soft.assertThatThrownBy(
            () -> persistenceSpi.finishedExpireContents(vals.id, vals.expiryFinish2, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Expected current status of EXPIRY_IN_PROGRESS, but is EXPIRY_FAILED");

    vals.assertExpiryFailure();
  }

  @Test
  public void nonExistingLiveSet() {
    UUID nonExisting = new UUID(0L, 0L);
    soft.assertThatThrownBy(() -> persistenceSpi.getLiveContentSet(nonExisting))
        .isInstanceOf(LiveContentSetNotFoundException.class)
        .hasMessage("Live content set " + nonExisting + " not found");

    try (Stream<LiveContentSet> liveContents = persistenceSpi.getAllLiveContents()) {
      soft.assertThat(liveContents).isEmpty();
    }

    soft.assertThatThrownBy(() -> persistenceSpi.deleteLiveContentSet(nonExisting))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not found");

    try (Stream<String> cids = persistenceSpi.fetchContentIds(nonExisting)) {
      soft.assertThat(cids).isEmpty();
    }

    try (Stream<ContentReference> refs =
        persistenceSpi.fetchContentReferences(nonExisting, "cid")) {
      soft.assertThat(refs).isEmpty();
    }

    try (Stream<ContentReference> refs =
        persistenceSpi.fetchContentReferences(nonExisting, "cid")) {
      soft.assertThat(refs).isEmpty();
    }

    soft.assertThat(persistenceSpi.fetchDistinctContentIdCount(nonExisting)).isEqualTo(0L);

    soft.assertThatThrownBy(
            () -> persistenceSpi.finishedIdentifyLiveContents(nonExisting, Instant.now(), null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not found");

    soft.assertThatThrownBy(() -> persistenceSpi.startExpireContents(nonExisting, Instant.now()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not found");

    soft.assertThatThrownBy(
            () -> persistenceSpi.finishedExpireContents(nonExisting, Instant.now(), null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not found");
  }

  @Test
  void identifyDuplicateContentReferences() throws Exception {
    LiveSetVals vals1 = new LiveSetVals();
    vals1.startIdentify();
    soft.assertThat(persistenceSpi.addIdentifiedLiveContent(vals1.id, vals1.refs.stream()))
        .isEqualTo(vals1.refs.size());
    for (ContentReference ref : vals1.refs) {
      soft.assertThat(persistenceSpi.addIdentifiedLiveContent(vals1.id, Stream.of(ref)))
          .isEqualTo(0L);
    }
    soft.assertThat(persistenceSpi.addIdentifiedLiveContent(vals1.id, vals1.refs.stream()))
        .isEqualTo(0L);
  }

  @Test
  void identifiedContents() throws Exception {
    LiveSetVals vals1 = new LiveSetVals();
    vals1.startIdentify();
    soft.assertThat(persistenceSpi.addIdentifiedLiveContent(vals1.id, vals1.refs.stream()))
        .isEqualTo(vals1.refs.size());
    soft.assertThat(persistenceSpi.fetchDistinctContentIdCount(vals1.id))
        .isEqualTo(vals1.refs.stream().map(ContentReference::contentId).distinct().count());
    try (Stream<String> contentIds = persistenceSpi.fetchContentIds(vals1.id)) {
      soft.assertThat(contentIds).containsExactlyInAnyOrderElementsOf(vals1.contentIds());
    }

    for (ContentReference ref : vals1.refs) {
      try (Stream<ContentReference> refs =
          persistenceSpi.fetchContentReferences(vals1.id, ref.contentId())) {
        soft.assertThat(refs)
            .containsExactlyInAnyOrderElementsOf(vals1.refsForCid(ref.contentId()));
      }
    }

    LiveSetVals vals2 = new LiveSetVals(15);
    vals2.startIdentify();
    soft.assertThat(persistenceSpi.addIdentifiedLiveContent(vals2.id, vals2.refs.stream()))
        .isEqualTo(vals2.refs.size());

    for (ContentReference ref : vals1.refs) {
      try (Stream<ContentReference> refs =
          persistenceSpi.fetchContentReferences(vals1.id, ref.contentId())) {
        soft.assertThat(refs)
            .containsExactlyInAnyOrderElementsOf(vals1.refsForCid(ref.contentId()));
      }
    }

    soft.assertThat(persistenceSpi.fetchDistinctContentIdCount(vals1.id))
        .isEqualTo(vals1.refs.stream().map(ContentReference::contentId).distinct().count());
    try (Stream<String> contentIds = persistenceSpi.fetchContentIds(vals1.id)) {
      soft.assertThat(contentIds).containsExactlyInAnyOrderElementsOf(vals1.contentIds());
    }

    for (ContentReference ref : vals2.refs) {
      try (Stream<ContentReference> refs =
          persistenceSpi.fetchContentReferences(vals2.id, ref.contentId())) {
        soft.assertThat(refs)
            .containsExactlyInAnyOrderElementsOf(vals2.refsForCid(ref.contentId()));
      }
    }

    soft.assertThat(persistenceSpi.fetchDistinctContentIdCount(vals2.id))
        .isEqualTo(vals2.refs.stream().map(ContentReference::contentId).distinct().count());
    try (Stream<String> contentIds = persistenceSpi.fetchContentIds(vals2.id)) {
      soft.assertThat(contentIds).containsExactlyInAnyOrderElementsOf(vals2.contentIds());
    }

    try (Stream<LiveContentSet> all = persistenceSpi.getAllLiveContents()) {
      soft.assertThat(all)
          .containsExactlyInAnyOrder(
              persistenceSpi.getLiveContentSet(vals1.id),
              persistenceSpi.getLiveContentSet(vals2.id));
    }
  }

  @Test
  public void baseLocations() throws Exception {
    LiveSetVals vals1 = new LiveSetVals();
    vals1.startIdentify();
    vals1.finishIdentify();
    vals1.startExpiry();

    LiveContentSet lcs = persistenceSpi.getLiveContentSet(vals1.id);

    lcs.associateBaseLocations(
        "cid",
        asList(
            StorageUri.of("file:///foo/bar"),
            StorageUri.of("file:///foo/bar"),
            StorageUri.of("file:///foo/bar")));
    lcs.associateBaseLocations(
        "cid",
        asList(
            StorageUri.of("file:///foo/bar1"),
            StorageUri.of("file:///foo/bar2"),
            StorageUri.of("file:///foo/bar3")));
    lcs.associateBaseLocations("cid", singletonList(StorageUri.of("file:///foo/bar1")));
    lcs.associateBaseLocations("cid", singletonList(StorageUri.of("file:///foo/bar2")));
    lcs.associateBaseLocations("cid", singletonList(StorageUri.of("file:///foo/bar3")));
    lcs.associateBaseLocations(
        "cid2",
        asList(
            StorageUri.of("file:///meep/foo/bar1"),
            StorageUri.of("file:///meep/foo/bar2"),
            StorageUri.of("file:///meep/foo/bar3")));

    try (Stream<StorageUri> locations = lcs.fetchBaseLocations("cid")) {
      soft.assertThat(locations)
          .containsExactlyInAnyOrder(
              StorageUri.of("file:///foo/bar"),
              StorageUri.of("file:///foo/bar1"),
              StorageUri.of("file:///foo/bar2"),
              StorageUri.of("file:///foo/bar3"));
    }
    try (Stream<StorageUri> locations = lcs.fetchBaseLocations("cid2")) {
      soft.assertThat(locations)
          .containsExactlyInAnyOrder(
              StorageUri.of("file:///meep/foo/bar1"),
              StorageUri.of("file:///meep/foo/bar2"),
              StorageUri.of("file:///meep/foo/bar3"));
    }

    try (Stream<StorageUri> locations = lcs.fetchAllBaseLocations()) {
      soft.assertThat(locations)
          .containsExactlyInAnyOrder(
              StorageUri.of("file:///foo/bar"),
              StorageUri.of("file:///foo/bar1"),
              StorageUri.of("file:///foo/bar2"),
              StorageUri.of("file:///foo/bar3"),
              StorageUri.of("file:///meep/foo/bar1"),
              StorageUri.of("file:///meep/foo/bar2"),
              StorageUri.of("file:///meep/foo/bar3"));
    }
  }

  @Test
  public void fileDeletions() throws Exception {
    LiveSetVals vals1 = new LiveSetVals();
    vals1.startIdentify();
    vals1.finishIdentify();
    vals1.startExpiry();

    List<FileReference> files =
        asList(
            FileReference.of(StorageUri.of("file1"), StorageUri.of("foo://bar/baz/"), 42L),
            // duplicate!
            FileReference.of(StorageUri.of("file1"), StorageUri.of("foo://bar/baz/"), 42L),
            FileReference.of(StorageUri.of("file2"), StorageUri.of("foo://bar/baz/"), 42L),
            FileReference.of(StorageUri.of("file3"), StorageUri.of("foo://meep/blah/"), 88L));

    Set<FileReference> expected = new HashSet<>(files);

    soft.assertThat(expected).hasSize(files.size() - 1);

    LiveContentSet lcs = persistenceSpi.getLiveContentSet(vals1.id);
    soft.assertThat(lcs.addFileDeletions(files.stream())).isEqualTo(expected.size());

    soft.assertThat(lcs.addFileDeletions(files.stream())).isEqualTo(0);

    vals1.finishExpiry();

    try (Stream<FileReference> deletions = lcs.fetchFileDeletions()) {
      soft.assertThat(deletions).containsExactlyInAnyOrderElementsOf(expected);
    }

    lcs.delete();

    assertDeleted(vals1.id);
  }

  protected abstract void assertDeleted(UUID id) throws Exception;
}
