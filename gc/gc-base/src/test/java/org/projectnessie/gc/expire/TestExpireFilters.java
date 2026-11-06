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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  /** What a sweep did. */
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

  // --- content-key filter (--target-namespace) ---------------------------------------------------

  /**
   * Only contents with a key in the namespace are processed, the others are left alone entirely.
   */
  @Test
  public void contentKeyFilterSelectsContentsByNamespace() throws Exception {
    SweepResult r =
        sweep(
            List.of(
                Table.of(ORDERS_ID, ContentKey.of("team_a", "orders"), ORDERS),
                Table.of(CUSTOMERS_ID, ContentKey.of("team_a", "customers"), CUSTOMERS),
                Table.of(EVENTS_ID, ContentKey.of("team_b", "events"), EVENTS)),
            inNamespace("team_a"),
            null);

    soft.assertThat(r.listed()).containsExactlyInAnyOrder(ORDERS, CUSTOMERS);
    soft.assertThat(r.deleted())
        .containsExactlyInAnyOrder(ORDERS.resolve("unused"), CUSTOMERS.resolve("unused"));
    // The content that is not selected is not touched at all, not even its base locations are
    // recorded.
    soft.assertThat(baseLocations(r.liveContentSet(), EVENTS_ID)).isEmpty();
    soft.assertThat(baseLocations(r.liveContentSet(), ORDERS_ID)).containsExactly(ORDERS);
  }

  /**
   * A table that was renamed into another namespace is selected by the namespace it is in now and
   * by the one it has been in, and it is swept as a whole either way: its files are in the folder
   * of the old namespace.
   */
  @Test
  public void renamedContentIsSelectedByEveryNamespaceItHasBeenIn() throws Exception {
    List<Table> tables =
        List.of(
            Table.of(
                CUSTOMERS_ID,
                List.of(
                    new Version(ContentKey.of("team_a", "customers"), "metadata"),
                    new Version(ContentKey.of("team_b", "customers"), "metadata")),
                CUSTOMERS));

    for (String namespace : List.of("team_a", "team_b")) {
      SweepResult r = sweep(tables, inNamespace(namespace), null);
      soft.assertThat(r.listed())
          .describedAs("listed for %s", namespace)
          .containsExactly(CUSTOMERS);
      soft.assertThat(r.deleted())
          .describedAs("deleted for %s", namespace)
          .containsExactly(CUSTOMERS.resolve("unused"));
    }

    SweepResult other = sweep(tables, inNamespace("team_c"), null);
    soft.assertThat(other.listed()).isEmpty();
    soft.assertThat(other.deleted()).isEmpty();
  }

  /**
   * Selection is by content, not by reference: of a selected content, the live files of all
   * versions are kept, also those of versions that were committed under a key that the filter does
   * not accept. Filtering the individual references instead would delete files that are in use.
   */
  @Test
  public void liveFilesOfAllVersionsOfSelectedContentSurvive() throws Exception {
    List<Table> tables =
        List.of(
            Table.of(
                CUSTOMERS_ID,
                List.of(
                    // an older version, committed before the table was moved to team_b
                    new Version(ContentKey.of("team_a", "customers"), "metadata-old"),
                    new Version(ContentKey.of("team_b", "customers"), "metadata-new")),
                CUSTOMERS));

    SweepResult r = sweep(tables, inNamespace("team_b"), null);

    soft.assertThat(r.listed()).containsExactly(CUSTOMERS);
    soft.assertThat(r.deleted()).containsExactly(CUSTOMERS.resolve("unused"));
    soft.assertThat(r.deleted())
        .doesNotContain(CUSTOMERS.resolve("metadata-old"), CUSTOMERS.resolve("metadata-new"));
  }

  /**
   * Both filters must accept: the table is selected via its namespace and its base location is
   * below the prefix.
   */
  @Test
  public void contentKeyFilterAndBaseLocationFilterCombine() throws Exception {
    SweepResult r =
        sweep(
            List.of(
                Table.of(ORDERS_ID, ContentKey.of("team_a", "orders"), ORDERS),
                // renamed from team_a to team_b, files are still in the folder of team_a
                Table.of(
                    CUSTOMERS_ID,
                    List.of(
                        new Version(ContentKey.of("team_a", "customers"), "metadata"),
                        new Version(ContentKey.of("team_b", "customers"), "metadata")),
                    CUSTOMERS),
                Table.of(EVENTS_ID, ContentKey.of("team_b", "events"), EVENTS)),
            inNamespace("team_b"),
            StoragePrefixes.atOrBelowAnyOf(List.of(StorageUri.of("mock://bucket/team_a/"))));

    // orders: not in team_b. events: in team_b, but its folder is not below the prefix.
    soft.assertThat(r.listed()).containsExactly(CUSTOMERS);
    soft.assertThat(r.deleted()).containsExactly(CUSTOMERS.resolve("unused"));
  }

  /** A content-key filter that accepts nothing processes nothing. */
  @Test
  public void contentKeyFilterAcceptingNothingSweepsNothing() throws Exception {
    SweepResult r =
        sweep(
            List.of(Table.of(ORDERS_ID, ContentKey.of("team_a", "orders"), ORDERS)),
            key -> false,
            null);

    soft.assertThat(r.listed()).isEmpty();
    soft.assertThat(r.deleted()).isEmpty();
    soft.assertThat(r.summary()).isEqualTo(DeleteSummary.EMPTY);
  }

  /** Matches the namespace of a key as a whole, like {@code --target-namespace} does. */
  static Predicate<ContentKey> inNamespace(String namespaceRegex) {
    Predicate<String> matches = Pattern.compile(namespaceRegex).asMatchPredicate();
    return key -> matches.test(key.getNamespace().toString());
  }

  // --- test fixture ---------------------------------------------------------------------------

  /** A version of a table: the key it was committed under and the name of its metadata file. */
  record Version(ContentKey key, String metadataFile) {}

  /** A table/view with the versions in the live-set and its base locations. */
  record Table(String contentId, List<Version> versions, List<StorageUri> bases) {
    static Table of(String contentId, ContentKey key, StorageUri... bases) {
      return new Table(contentId, List.of(new Version(key, "metadata")), List.of(bases));
    }

    static Table of(String contentId, List<Version> versions, StorageUri... bases) {
      return new Table(contentId, versions, List.of(bases));
    }
  }

  /** Convenience for tests that only use base locations and a base-location filter. */
  SweepResult sweep(
      Map<String, List<StorageUri>> basesPerContent, Predicate<StorageUri> baseLocationFilter)
      throws Exception {
    List<Table> tables =
        basesPerContent.entrySet().stream()
            .map(
                e ->
                    new Table(
                        e.getKey(),
                        List.of(new Version(ContentKey.of("ns", e.getKey()), "metadata")),
                        e.getValue()))
            .collect(Collectors.toList());
    return sweep(tables, null, baseLocationFilter);
  }

  /**
   * Runs the sweep phase over the given tables.
   *
   * <p>Each version of a table is live, with one metadata file per base location of the table. Each
   * base location holds the metadata files of its tables, an old {@code unused} file (to be
   * deleted), and a {@code too-new} file (never deleted).
   *
   * @param contentKeyFilter filter to apply, {@code null} for none
   * @param baseLocationFilter filter to apply, {@code null} for none
   */
  SweepResult sweep(
      List<Table> tables,
      Predicate<ContentKey> contentKeyFilter,
      Predicate<StorageUri> baseLocationFilter)
      throws Exception {
    Instant now = Instant.now();
    Instant maxFileModificationTime = now.minus(1, ChronoUnit.SECONDS);
    long oldMillis = maxFileModificationTime.toEpochMilli() - 10_000L;

    Map<String, Table> tablesById =
        tables.stream().collect(Collectors.toMap(Table::contentId, t -> t));

    InMemoryPersistenceSpi storage = new InMemoryPersistenceSpi();
    UUID id = UUID.randomUUID();
    storage.startIdentifyLiveContents(id, now);
    storage.addIdentifiedLiveContent(
        id,
        tables.stream()
            .flatMap(
                table ->
                    IntStream.range(0, table.versions().size())
                        .mapToObj(
                            i ->
                                icebergContent(
                                    ICEBERG_TABLE,
                                    table.contentId(),
                                    "commit-" + table.contentId() + "-" + i,
                                    table.versions().get(i).key(),
                                    table
                                        .bases()
                                        .get(0)
                                        .resolve(table.versions().get(i).metadataFile())
                                        .toString(),
                                    42L))));
    storage.finishedIdentifyLiveContents(id, now, null);
    LiveContentSet liveContentSet = storage.getLiveContentSet(id);

    // The live files of a version: its metadata file in each base location of the table.
    ContentToFiles contentToFiles =
        ref -> {
          String metadataFile =
              ref.metadataLocation().substring(ref.metadataLocation().lastIndexOf('/') + 1);
          return tablesById.get(ref.contentId()).bases().stream()
              .map(base -> FileReference.of(StorageUri.of(metadataFile), base, -1L));
        };

    Set<StorageUri> listed = ConcurrentHashMap.newKeySet();
    FilesLister lister =
        base -> {
          listed.add(base);
          Stream<FileReference> metadataFiles =
              tables.stream()
                  .filter(t -> t.bases().contains(base))
                  .flatMap(t -> t.versions().stream())
                  .map(Version::metadataFile)
                  .distinct()
                  .map(name -> FileReference.of(StorageUri.of(name), base, oldMillis));
          return Stream.concat(
              metadataFiles,
              Stream.of(
                  FileReference.of(StorageUri.of("unused"), base, oldMillis),
                  FileReference.of(
                      StorageUri.of("too-new"), base, maxFileModificationTime.toEpochMilli() + 1)));
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
    if (contentKeyFilter != null) {
      parameters.contentKeyFilter(contentKeyFilter);
    }
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
