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
package org.projectnessie.gc.roundtrip;

import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.gc.identify.CutoffPolicy.numCommits;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.expire.ExpireParameters;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.identify.ContentTypeFilter;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Runs the mark phase and the sweep phase over a small commit history in which a table is renamed
 * into another namespace, with its files staying in the folder of the first namespace.
 *
 * <p>The mark phase always covers the whole catalog. A sweep that is restricted to some namespaces
 * must neither miss that table, nor delete any file that one of its live versions refers to.
 *
 * <pre>
 *   commit 1  put   team_b.events     events/metadata-1
 *   commit 2  put   team_a.orders     orders/metadata-1
 *   commit 3  put   team_a.customers  customers/metadata-1   (cut off, not live)
 *   commit 4  put   team_a.customers  customers/metadata-2   (compaction, metadata-1 unreferenced)
 *   commit 5  rename team_a.customers to team_b.customers, same metadata, files stay in team_a/
 * </pre>
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestSweepFiltersAfterRename {
  @InjectSoftAssertions SoftAssertions soft;

  static final StorageUri EVENTS_DIR = StorageUri.of("mock://bucket/team_b/events/");
  static final StorageUri ORDERS_DIR = StorageUri.of("mock://bucket/team_a/orders/");
  static final StorageUri CUSTOMERS_DIR = StorageUri.of("mock://bucket/team_a/customers/");

  static final String EVENTS_ID = "cid-events";
  static final String ORDERS_ID = "cid-orders";
  static final String CUSTOMERS_ID = "cid-customers";

  static final Map<String, StorageUri> DIR_BY_CONTENT_ID =
      Map.of(EVENTS_ID, EVENTS_DIR, ORDERS_ID, ORDERS_DIR, CUSTOMERS_ID, CUSTOMERS_DIR);

  static final ContentKey EVENTS = ContentKey.of("team_b", "events");
  static final ContentKey ORDERS = ContentKey.of("team_a", "orders");
  static final ContentKey CUSTOMERS_OLD = ContentKey.of("team_a", "customers");
  static final ContentKey CUSTOMERS_NEW = ContentKey.of("team_b", "customers");

  static final Set<Content.Type> TYPES = ImmutableSet.of(ICEBERG_TABLE);

  final Instant now = Instant.now();
  final Instant maxFileModificationTime = now.minus(1, ChronoUnit.SECONDS);

  /** The mark phase is not restricted: all tables are live, the renamed table with both keys. */
  @Test
  public void markCoversWholeCatalogAndKeepsAllKeysOfRenamedTable() throws Exception {
    LiveContentSet liveContentSet = mark();

    try (Stream<String> contentIds = liveContentSet.fetchContentIds()) {
      soft.assertThat(contentIds).containsExactlyInAnyOrder(EVENTS_ID, ORDERS_ID, CUSTOMERS_ID);
    }
    try (Stream<ContentReference> refs = liveContentSet.fetchContentReferences(CUSTOMERS_ID)) {
      soft.assertThat(refs.map(ContentReference::contentKey))
          .containsExactlyInAnyOrder(CUSTOMERS_OLD, CUSTOMERS_NEW);
    }
  }

  /** Control: without a restriction, the sweep behaves as before. */
  @Test
  public void sweepWithoutFilter() throws Exception {
    soft.assertThat(sweepDeleted(null))
        .containsExactlyInAnyOrder(
            CUSTOMERS_DIR.resolve("metadata-1"),
            CUSTOMERS_DIR.resolve("unused"),
            ORDERS_DIR.resolve("unused"),
            EVENTS_DIR.resolve("unused"));
  }

  /**
   * The namespace the table has been renamed into selects it. Its files are in the folder of the
   * other namespace, and the old version's files are deleted, those of the live version are not.
   */
  @Test
  public void sweepForNamespaceTheTableWasRenamedInto() throws Exception {
    Set<StorageUri> deleted = sweepDeleted("team_b");

    soft.assertThat(deleted)
        .containsExactlyInAnyOrder(
            CUSTOMERS_DIR.resolve("metadata-1"),
            CUSTOMERS_DIR.resolve("unused"),
            EVENTS_DIR.resolve("unused"));
    soft.assertThat(deleted).doesNotContain(CUSTOMERS_DIR.resolve("metadata-2"));
  }

  /**
   * The namespace the table has been renamed out of still selects it, because the live-set has
   * references to it under that key. orders is selected too, events is left alone.
   */
  @Test
  public void sweepForNamespaceTheTableWasRenamedOutOf() throws Exception {
    Set<StorageUri> deleted = sweepDeleted("team_a");

    soft.assertThat(deleted)
        .containsExactlyInAnyOrder(
            CUSTOMERS_DIR.resolve("metadata-1"),
            CUSTOMERS_DIR.resolve("unused"),
            ORDERS_DIR.resolve("unused"));
    soft.assertThat(deleted).doesNotContain(CUSTOMERS_DIR.resolve("metadata-2"));
  }

  /** A namespace without tables selects nothing, so nothing is deleted. */
  @Test
  public void sweepForUnrelatedNamespaceDeletesNothing() throws Exception {
    soft.assertThat(sweepDeleted("team_c")).isEmpty();
  }

  /** A pattern for several namespaces selects the tables of all of them. */
  @Test
  public void sweepForSeveralNamespaces() throws Exception {
    soft.assertThat(sweepDeleted("team_a|team_b"))
        .containsExactlyInAnyOrder(
            CUSTOMERS_DIR.resolve("metadata-1"),
            CUSTOMERS_DIR.resolve("unused"),
            ORDERS_DIR.resolve("unused"),
            EVENTS_DIR.resolve("unused"));
  }

  // --- fixture --------------------------------------------------------------------------------

  /** The commits of the history above, oldest first. */
  private List<LogEntry> history() {
    List<List<Operation>> operations =
        List.of(
            List.of(Operation.Put.of(EVENTS, table(EVENTS_ID, EVENTS_DIR, 1))),
            List.of(Operation.Put.of(ORDERS, table(ORDERS_ID, ORDERS_DIR, 1))),
            List.of(Operation.Put.of(CUSTOMERS_OLD, table(CUSTOMERS_ID, CUSTOMERS_DIR, 1))),
            List.of(Operation.Put.of(CUSTOMERS_OLD, table(CUSTOMERS_ID, CUSTOMERS_DIR, 2))),
            List.of(
                Operation.Delete.of(CUSTOMERS_OLD),
                Operation.Put.of(CUSTOMERS_NEW, table(CUSTOMERS_ID, CUSTOMERS_DIR, 2))));

    List<LogEntry> entries = new ArrayList<>();
    for (int i = 0; i < operations.size(); i++) {
      int commit = i + 1;
      entries.add(
          LogEntry.builder()
              .commitMeta(
                  CommitMeta.builder()
                      .commitTime(now.minus(operations.size() - commit, ChronoUnit.MINUTES))
                      .hash(hash(commit))
                      .message("commit " + commit)
                      .build())
              .parentCommitHash(hash(commit - 1))
              .addAllOperations(operations.get(i))
              .build());
    }
    return entries;
  }

  private static String hash(int commit) {
    return String.format("%016x", commit);
  }

  private static IcebergTable table(String contentId, StorageUri dir, int metadataVersion) {
    return IcebergTable.of(
        dir.resolve("metadata-" + metadataVersion).toString(), metadataVersion, 1, 2, 3, contentId);
  }

  /** Runs the mark phase over the whole catalog. Only the 2 newest commits of 'main' are live. */
  private LiveContentSet mark() throws Exception {
    List<LogEntry> history = history();
    InMemoryPersistenceSpi storage = new InMemoryPersistenceSpi();
    LiveContentSetsRepository repository =
        LiveContentSetsRepository.builder().persistenceSpi(storage).build();

    IdentifyLiveContents identify =
        IdentifyLiveContents.builder()
            .contentTypeFilter(
                new ContentTypeFilter() {
                  @Override
                  public boolean test(Content.Type type) {
                    return TYPES.contains(type);
                  }

                  @Override
                  public Set<Content.Type> validTypes() {
                    return TYPES;
                  }
                })
            .cutOffPolicySupplier(ref -> numCommits(2))
            .contentToContentReference(
                (content, commitId, key) -> icebergContent(commitId, key, content))
            .liveContentSetsRepository(repository)
            .parallelism(1)
            .repositoryConnector(
                new RepositoryConnector() {
                  @Override
                  public Stream<Reference> allReferences() {
                    return Stream.of(Branch.of("main", hash(history.size())));
                  }

                  @Override
                  public Stream<LogEntry> commitLog(Reference ref) {
                    // newest first
                    List<LogEntry> newestFirst = new ArrayList<>(history);
                    Collections.reverse(newestFirst);
                    return newestFirst.stream();
                  }

                  @Override
                  public Stream<Map.Entry<ContentKey, Content>> allContents(
                      Detached ref, Set<Content.Type> types) {
                    // The state of the catalog right after the requested commit
                    int upTo = Integer.parseInt(ref.getHash(), 16);
                    Map<ContentKey, Content> state = new LinkedHashMap<>();
                    for (LogEntry entry : history.subList(0, upTo)) {
                      for (Operation op : entry.getOperations()) {
                        if (op instanceof Operation.Put put) {
                          state.put(put.getKey(), put.getContent());
                        } else if (op instanceof Operation.Delete delete) {
                          state.remove(delete.getKey());
                        }
                      }
                    }
                    return state.entrySet().stream()
                        .filter(e -> types.contains(e.getValue().getType()));
                  }

                  @Override
                  public void close() {}
                })
            .build();

    UUID id = identify.identifyLiveContents();
    return repository.getLiveContentSet(id);
  }

  /**
   * Marks the whole catalog, then sweeps with a content-key filter for the namespace, if given.
   *
   * <p>Every folder holds all metadata files of its tables ({@code metadata-1}, and {@code
   * metadata-2} for customers), an {@code unused} file, and a {@code too-new} file.
   *
   * @return the deleted files
   */
  private Set<StorageUri> sweepDeleted(String namespaceRegex) throws Exception {
    LiveContentSet liveContentSet = mark();

    Set<StorageUri> deleted = ConcurrentHashMap.newKeySet();
    long oldMillis = maxFileModificationTime.toEpochMilli() - 10_000L;
    ExpireParameters.Builder parameters =
        ExpireParameters.builder()
            .liveContentSet(liveContentSet)
            .expectedFileCount(100)
            .maxFileModificationTime(maxFileModificationTime)
            .contentToFiles(
                ref -> {
                  StorageUri dir = DIR_BY_CONTENT_ID.get(ref.contentId());
                  return Stream.of(
                      FileReference.of(
                          dir.relativize(StorageUri.of(ref.metadataLocation())), dir, -1L));
                })
            .filesLister(
                dir -> {
                  List<FileReference> files = new ArrayList<>();
                  files.add(FileReference.of(StorageUri.of("metadata-1"), dir, oldMillis));
                  if (dir.equals(CUSTOMERS_DIR)) {
                    files.add(FileReference.of(StorageUri.of("metadata-2"), dir, oldMillis));
                  }
                  files.add(FileReference.of(StorageUri.of("unused"), dir, oldMillis));
                  files.add(
                      FileReference.of(
                          StorageUri.of("too-new"),
                          dir,
                          maxFileModificationTime.toEpochMilli() + 1));
                  return files.stream();
                })
            .fileDeleter(
                file -> {
                  deleted.add(file.absolutePath());
                  return DeleteResult.SUCCESS;
                });
    if (namespaceRegex != null) {
      Predicate<String> matches = Pattern.compile(namespaceRegex).asMatchPredicate();
      parameters.contentKeyFilter(key -> matches.test(key.getNamespace().toString()));
    }

    DefaultLocalExpire.builder().expireParameters(parameters.build()).build().expire();

    soft.assertThat(deleted)
        .describedAs("a file that is too new must never be deleted")
        .doesNotContain(
            EVENTS_DIR.resolve("too-new"),
            ORDERS_DIR.resolve("too-new"),
            CUSTOMERS_DIR.resolve("too-new"));
    soft.assertThat(deleted)
        .describedAs("live files must never be deleted")
        .doesNotContain(
            EVENTS_DIR.resolve("metadata-1"),
            ORDERS_DIR.resolve("metadata-1"),
            CUSTOMERS_DIR.resolve("metadata-2"));
    return deleted.stream().collect(Collectors.toSet());
  }
}
