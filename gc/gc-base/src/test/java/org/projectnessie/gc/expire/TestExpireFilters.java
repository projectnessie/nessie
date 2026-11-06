/*
 * Copyright (C) 2026 Dremio
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

import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.model.ContentKey;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Verifies the filters that restrict the sweep phase to a subset of the contents resp. their base
 * locations.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestExpireFilters {
  @InjectSoftAssertions SoftAssertions soft;

  static final StorageUri ORDERS = StorageUri.of("mock://bucket/team_a/orders/");
  static final StorageUri CUSTOMERS = StorageUri.of("mock://bucket/team_a/customers/");
  static final StorageUri EVENTS = StorageUri.of("mock://bucket/team_b/events/");

  static final String ORDERS_ID = "cid-orders";
  static final String CUSTOMERS_ID = "cid-customers";
  static final String EVENTS_ID = "cid-events";

  /** What a {@link #sweep(Map, Predicate) sweep} did. */
  record SweepResult(
      LiveContentSet liveContentSet,
      DeleteSummary summary,
      Set<StorageUri> listed,
      Set<StorageUri> deleted) {}

  /** Without a filter, all base locations of all contents are swept. */
  @Test
  public void noFilterSweepsEverything() throws Exception {
    SweepResult r =
        sweep(
            Map.of(
                ORDERS_ID,
                List.of(ORDERS),
                CUSTOMERS_ID,
                List.of(CUSTOMERS),
                EVENTS_ID,
                List.of(EVENTS)),
            null);

    soft.assertThat(r.listed()).containsExactlyInAnyOrder(ORDERS, CUSTOMERS, EVENTS);
    soft.assertThat(r.deleted())
        .containsExactlyInAnyOrder(
            ORDERS.resolve("unused"), CUSTOMERS.resolve("unused"), EVENTS.resolve("unused"));
    soft.assertThat(r.summary())
        .extracting(DeleteSummary::deleted, DeleteSummary::failures)
        .containsExactly(3L, 0L);
  }

  /** Only base locations at or below the prefix are listed and have files deleted. */
  @Test
  public void baseLocationFilterRestrictsSweepToPrefix() throws Exception {
    SweepResult r =
        sweep(
            Map.of(
                ORDERS_ID,
                List.of(ORDERS),
                CUSTOMERS_ID,
                List.of(CUSTOMERS),
                EVENTS_ID,
                List.of(EVENTS)),
            StoragePrefixes.atOrBelowAnyOf(List.of(StorageUri.of("mock://bucket/team_a/"))));

    soft.assertThat(r.listed()).containsExactlyInAnyOrder(ORDERS, CUSTOMERS);
    soft.assertThat(r.deleted())
        .containsExactlyInAnyOrder(ORDERS.resolve("unused"), CUSTOMERS.resolve("unused"));
    soft.assertThat(r.summary())
        .extracting(DeleteSummary::deleted, DeleteSummary::failures)
        .containsExactly(2L, 0L);

    // The base locations of all contents are still recorded in the live-set, also those not swept.
    soft.assertThat(baseLocations(r.liveContentSet(), EVENTS_ID)).containsExactly(EVENTS);
    soft.assertThat(baseLocations(r.liveContentSet(), ORDERS_ID)).containsExactly(ORDERS);
  }

  /** A prefix only matches whole path segments, '/team_a' must not select '/team_ab/'. */
  @Test
  public void prefixDoesNotMatchSiblingWithCommonNamePrefix() throws Exception {
    StorageUri sibling = StorageUri.of("mock://bucket/team_ab/orders/");

    SweepResult r =
        sweep(
            Map.of(ORDERS_ID, List.of(ORDERS), EVENTS_ID, List.of(sibling)),
            StoragePrefixes.atOrBelowAnyOf(List.of(StorageUri.of("mock://bucket/team_a"))));

    soft.assertThat(r.listed()).containsExactly(ORDERS);
    soft.assertThat(r.deleted()).containsExactly(ORDERS.resolve("unused"));
  }

  /**
   * A table located above the prefix, for example at the root of the warehouse, is skipped:
   * sweeping it would list and delete outside of the prefix.
   */
  @Test
  public void baseLocationAbovePrefixIsSkipped() throws Exception {
    StorageUri warehouseRoot = StorageUri.of("mock://bucket/");

    SweepResult r =
        sweep(
            Map.of(ORDERS_ID, List.of(ORDERS), EVENTS_ID, List.of(warehouseRoot)),
            StoragePrefixes.atOrBelowAnyOf(List.of(StorageUri.of("mock://bucket/team_a/"))));

    soft.assertThat(r.listed()).containsExactly(ORDERS);
    soft.assertThat(r.deleted()).containsExactly(ORDERS.resolve("unused"));
    soft.assertThat(baseLocations(r.liveContentSet(), EVENTS_ID)).containsExactly(warehouseRoot);
  }

  /** Of a content with several base locations, for example after a move, only the matching ones. */
  @Test
  public void onlyMatchingBaseLocationsOfOneContentAreSwept() throws Exception {
    StorageUri oldLocation = StorageUri.of("mock://bucket/team_b/old-orders/");

    SweepResult r =
        sweep(
            Map.of(ORDERS_ID, List.of(ORDERS, oldLocation)),
            StoragePrefixes.atOrBelowAnyOf(List.of(StorageUri.of("mock://bucket/team_a/"))));

    soft.assertThat(r.listed()).containsExactly(ORDERS);
    soft.assertThat(r.deleted()).containsExactly(ORDERS.resolve("unused"));
    soft.assertThat(baseLocations(r.liveContentSet(), ORDERS_ID))
        .containsExactlyInAnyOrder(ORDERS, oldLocation);
  }

  /** A filter that accepts nothing lists and deletes nothing. */
  @Test
  public void filterAcceptingNothingSweepsNothing() throws Exception {
    SweepResult r =
        sweep(
            Map.of(ORDERS_ID, List.of(ORDERS), EVENTS_ID, List.of(EVENTS)),
            StoragePrefixes.atOrBelowAnyOf(List.of()));

    soft.assertThat(r.listed()).isEmpty();
    soft.assertThat(r.deleted()).isEmpty();
    soft.assertThat(r.summary()).isEqualTo(DeleteSummary.EMPTY);
  }

  /** Live files are never deleted, filter or not. */
  @Test
  public void liveFilesSurviveRestrictedSweep() throws Exception {
    SweepResult r =
        sweep(
            Map.of(ORDERS_ID, List.of(ORDERS)),
            StoragePrefixes.atOrBelowAnyOf(List.of(StorageUri.of("mock://bucket/team_a/"))));

    soft.assertThat(r.deleted()).doesNotContain(ORDERS.resolve("metadata"));
  }

  /**
   * Runs the sweep phase over contents with the given base locations.
   *
   * <p>Each content is live with one metadata file per base location, and each base location holds
   * that live {@code metadata} file, an old {@code unused} file (to be deleted), and a {@code
   * too-new} file (never deleted).
   *
   * @param basesPerContent base locations per content-ID
   * @param baseLocationFilter filter to apply, {@code null} for none
   */
  SweepResult sweep(
      Map<String, List<StorageUri>> basesPerContent, Predicate<StorageUri> baseLocationFilter)
      throws Exception {
    Instant now = Instant.now();
    Instant maxFileModificationTime = now.minus(1, ChronoUnit.SECONDS);
    long oldMillis = maxFileModificationTime.toEpochMilli() - 10_000L;

    InMemoryPersistenceSpi storage = new InMemoryPersistenceSpi();
    UUID id = UUID.randomUUID();
    storage.startIdentifyLiveContents(id, now);
    Map<String, ContentKey> keys = new LinkedHashMap<>();
    basesPerContent.keySet().forEach(cid -> keys.put(cid, ContentKey.of("ns", cid)));
    storage.addIdentifiedLiveContent(
        id,
        basesPerContent.keySet().stream()
            .map(
                cid ->
                    icebergContent(
                        ICEBERG_TABLE,
                        cid,
                        "commit-" + cid,
                        keys.get(cid),
                        basesPerContent.get(cid).get(0).resolve("metadata").toString(),
                        42L)));
    storage.finishedIdentifyLiveContents(id, now, null);
    LiveContentSet liveContentSet = storage.getLiveContentSet(id);

    ContentToFiles contentToFiles =
        ref ->
            basesPerContent.get(ref.contentId()).stream()
                .map(base -> FileReference.of(StorageUri.of("metadata"), base, -1L));

    Set<StorageUri> listed = ConcurrentHashMap.newKeySet();
    FilesLister lister =
        base -> {
          listed.add(base);
          return Stream.of(
              FileReference.of(StorageUri.of("metadata"), base, oldMillis),
              FileReference.of(StorageUri.of("unused"), base, oldMillis),
              FileReference.of(
                  StorageUri.of("too-new"), base, maxFileModificationTime.toEpochMilli() + 1));
        };

    Set<StorageUri> deleted = ConcurrentHashMap.newKeySet();
    FileDeleter deleter =
        fileObject -> {
          deleted.add(fileObject.absolutePath());
          return DeleteResult.SUCCESS;
        };

    ExpireParameters.Builder parameters =
        ExpireParameters.builder()
            .liveContentSet(liveContentSet)
            .filesLister(lister)
            .fileDeleter(deleter)
            .expectedFileCount(100)
            .contentToFiles(contentToFiles)
            .maxFileModificationTime(maxFileModificationTime);
    if (baseLocationFilter != null) {
      parameters.baseLocationFilter(baseLocationFilter);
    }

    DeleteSummary summary =
        DefaultLocalExpire.builder().expireParameters(parameters.build()).build().expire();

    return new SweepResult(liveContentSet, summary, listed, deleted);
  }

  static List<StorageUri> baseLocations(LiveContentSet liveContentSet, String contentId) {
    try (Stream<StorageUri> baseLocations = liveContentSet.fetchBaseLocations(contentId)) {
      return baseLocations.collect(Collectors.toList());
    }
  }
}
