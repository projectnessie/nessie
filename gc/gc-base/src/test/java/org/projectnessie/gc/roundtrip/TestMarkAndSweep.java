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
package org.projectnessie.gc.roundtrip;

import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.gc.identify.CutoffPolicy.atTimestamp;
import static org.projectnessie.gc.identify.CutoffPolicy.numCommits;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSet.Status;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.expire.ExpireParameters;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.identify.ContentTypeFilter;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMarkAndSweep {
  public static final ImmutableSet<Content.Type> ICEBERG_CONTENT_TYPES =
      ImmutableSet.of(ICEBERG_TABLE, ICEBERG_VIEW);

  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<Arguments> markAndSweep() {
    return Stream.of(
        Arguments.arguments(new MarkAndSweep(100L, 10, 2L, false)),
        Arguments.arguments(new MarkAndSweep(100L, 10, 2L, true)),
        Arguments.arguments(new MarkAndSweep(100_000L, 10_000, 2L, false)),
        Arguments.arguments(new MarkAndSweep(100_000L, 10_000, 2L, true)));
  }

  static class MarkAndSweep {
    final long numCommits;
    final int numKeysAtCutOff;
    final long numExpired;
    final String cidPrefix = "cid-";
    final StorageUri basePath = StorageUri.of("meep://host-and-port/data/lake/");
    final Instant now = Instant.now();
    final Instant maxFileModificationTime;
    final long newestToDeleteMillis;
    final long tooNewMillis;
    final boolean atTimestamp;

    MarkAndSweep(long numCommits, int numKeysAtCutOff, long numExpired, boolean atTimestamp) {
      this.numCommits = numCommits;
      this.numKeysAtCutOff = numKeysAtCutOff;
      this.numExpired = numExpired;
      this.maxFileModificationTime = now.minus(1, ChronoUnit.SECONDS);
      this.newestToDeleteMillis = maxFileModificationTime.toEpochMilli();
      this.tooNewMillis = maxFileModificationTime.toEpochMilli() + 1;
      this.atTimestamp = atTimestamp;
    }

    @Override
    public String toString() {
      return "CommitsAndContentsSimulation{"
          + "numCommits="
          + numCommits
          + ", numKeysAtCutOff="
          + numKeysAtCutOff
          + ", numExpired="
          + numExpired
          + ", atTimestamp="
          + atTimestamp
          + '}';
    }

    String numToHash(long l) {
      return String.format("%016x", l);
    }

    String numToContentId(long l) {
      return cidPrefix + l;
    }

    StorageUri numToBaseLocation(long l) {
      return basePath.resolve(l + "/");
    }

    long hashToNum(Reference ref) {
      return Long.parseLong(ref.getHash(), 16);
    }

    long contentIdToNum(String cid) {
      return Long.parseLong(cid.substring(cidPrefix.length()));
    }

    long baseLocationToNum(StorageUri path) {
      String p = path.location();
      int l = p.lastIndexOf('/');
      int l1 = p.lastIndexOf('/', l - 1);
      return Long.parseLong(p.substring(l1 + 1, l));
    }

    Content numToContent(long l) {
      if ((l & 1L) == 1L) {
        return IcebergView.of(
            numToContentId(l), numToBaseLocation(l).resolve("metadata-" + l).toString(), l, 1);
      }
      return IcebergTable.of(
          numToBaseLocation(l).resolve("metadata-" + l).toString(), l, 1, 2, 3, numToContentId(l));
    }

    ContentKey numToContentKey(long l) {
      return ContentKey.of("content", Long.toString(l));
    }

    LogEntry numToLogEntry(long l) {
      return LogEntry.builder()
          .commitMeta(
              CommitMeta.builder()
                  .commitTime(now.minus(numCommits - l, ChronoUnit.SECONDS))
                  .hash(numToHash(l))
                  .message("hello")
                  .build())
          .parentCommitHash(numToHash(l - 1))
          .addOperations(Put.of(numToContentKey(l), numToContent(l)))
          .build();
    }

    long liveContentsCount() {
      return numCommits + numKeysAtCutOff - numExpired;
    }

    PerRefCutoffPolicySupplier cutOffPolicySupplier() {
      Instant cutOff = now.minus(numCommits - numExpired, ChronoUnit.SECONDS);
      return atTimestamp ? r -> atTimestamp(cutOff) : r -> numCommits((int) numCommits - 1);
    }
  }

  @ParameterizedTest
  @MethodSource("markAndSweep")
  void markAndSweep(MarkAndSweep markAndSweep) throws Exception {

    InMemoryPersistenceSpi storage = new InMemoryPersistenceSpi();
    LiveContentSetsRepository repository =
        LiveContentSetsRepository.builder().persistenceSpi(storage).build();
    IdentifyLiveContents identify =
        IdentifyLiveContents.builder()
            .contentTypeFilter(
                new ContentTypeFilter() {
                  @Override
                  public boolean test(Content.Type type) {
                    return ICEBERG_CONTENT_TYPES.contains(type);
                  }

                  @Override
                  public Set<Content.Type> validTypes() {
                    return ICEBERG_CONTENT_TYPES;
                  }
                })
            .cutOffPolicySupplier(markAndSweep.cutOffPolicySupplier())
            .contentToContentReference(
                (content, commitId, key) -> icebergContent(commitId, key, content))
            .liveContentSetsRepository(repository)
            .repositoryConnector(
                new RepositoryConnector() {
                  @Override
                  public Stream<Reference> allReferences() {
                    return Stream.of(
                        Branch.of("main", markAndSweep.numToHash(markAndSweep.numCommits)));
                  }

                  @Override
                  public Stream<LogEntry> commitLog(Reference ref) {
                    long num = markAndSweep.hashToNum(ref);
                    if (num < 0L) {
                      return Stream.empty();
                    }
                    return LongStream.range(0, num)
                        .map(l -> num - l)
                        .mapToObj(markAndSweep::numToLogEntry);
                  }

                  @Override
                  public Stream<Map.Entry<ContentKey, Content>> allContents(
                      Detached ref, Set<Content.Type> types) {

                    long l = markAndSweep.hashToNum(ref);
                    // 1L is the very first, commit - the first non-live commit
                    // 2L is the oldest live-commit - need to fetch the visible keys from that one
                    soft.assertThat(l).isEqualTo(2L);
                    List<ContentKey> keys =
                        IntStream.range(0, markAndSweep.numKeysAtCutOff)
                            .mapToObj(
                                i -> markAndSweep.numToContentKey(markAndSweep.numCommits + i))
                            .collect(Collectors.toList());
                    soft.assertThat(keys).hasSize(markAndSweep.numKeysAtCutOff);
                    return keys.stream()
                        .map(
                            ck ->
                                Maps.immutableEntry(
                                    ck,
                                    markAndSweep.numToContent(
                                        Long.parseLong(ck.getElements().get(1)))));
                  }

                  @Override
                  public void close() {}
                })
            .build();

    UUID id = identify.identifyLiveContents();

    LiveContentSet liveContentSet = repository.getLiveContentSet(id);

    soft.assertThat(liveContentSet)
        .extracting(LiveContentSet::status, LiveContentSet::fetchDistinctContentIdCount)
        .containsExactly(Status.IDENTIFY_SUCCESS, markAndSweep.liveContentsCount());

    try (Stream<String> contentIds = liveContentSet.fetchContentIds()) {
      soft.assertThat(contentIds)
          .containsExactlyInAnyOrderElementsOf(
              LongStream.range(
                      markAndSweep.numExpired,
                      markAndSweep.numCommits + markAndSweep.numKeysAtCutOff)
                  .mapToObj(markAndSweep::numToContentId)
                  .collect(Collectors.toSet()));
    }

    soft.assertAll();

    // Retrieves 4 files per base directory:
    // * metadata (emitted by the ContentToFiles implementation)
    // * one file that is too new (simulating a file that's been created by a concurrent operation)
    // * two that are unused --> those two must be deleted.
    FilesLister lister =
        path -> {
          long l = markAndSweep.baseLocationToNum(path);
          return Stream.of(
              FileReference.of(StorageUri.of("metadata-" + l), path, 123L),
              FileReference.of(StorageUri.of("1-unused"), path, 123L),
              FileReference.of(StorageUri.of("2-unused"), path, markAndSweep.newestToDeleteMillis),
              FileReference.of(StorageUri.of("too-new-2"), path, markAndSweep.tooNewMillis));
        };
    FileDeleter deleter =
        fileObject -> {
          soft.assertThat(fileObject.path().location()).endsWith("-unused");
          return DeleteResult.SUCCESS;
        };

    DefaultLocalExpire localExpire =
        DefaultLocalExpire.builder()
            .expireParameters(
                ExpireParameters.builder()
                    .liveContentSet(liveContentSet)
                    .filesLister(lister)
                    .fileDeleter(deleter)
                    .expectedFileCount(100)
                    .contentToFiles(
                        contentReference -> {
                          StorageUri baseLocation =
                              markAndSweep.numToBaseLocation(
                                  markAndSweep.contentIdToNum(contentReference.contentId()));
                          StorageUri location = StorageUri.of(contentReference.metadataLocation());
                          return Stream.of(
                              FileReference.of(
                                  baseLocation.relativize(location), baseLocation, -1L));
                        })
                    .maxFileModificationTime(markAndSweep.maxFileModificationTime)
                    .build())
            .build();

    DeleteSummary deleteSummary = localExpire.expire();
    soft.assertThat(deleteSummary)
        .extracting(DeleteSummary::deleted, DeleteSummary::failures)
        .containsExactly(markAndSweep.liveContentsCount() * 2L, 0L);

    soft.assertThat(repository.getLiveContentSet(id))
        .extracting(LiveContentSet::status)
        .isEqualTo(Status.EXPIRY_SUCCESS);
  }
}
