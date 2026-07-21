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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 * Verifies that expiry can handle live files that are referenced via absolute URIs, because they
 * are not located under the content's base location, see <a
 * href="https://github.com/projectnessie/nessie/issues/10817">issue #10817</a>.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestExpireAbsolutePaths {
  @InjectSoftAssertions SoftAssertions soft;

  static final StorageUri BASE = StorageUri.of("mock://bucket/table/");
  static final String CONTENT_ID = "cid-1";

  @Test
  public void absolutePathsSurviveExpiry() throws Exception {
    Instant now = Instant.now();
    Instant maxFileModificationTime = now.minus(1, ChronoUnit.SECONDS);
    long oldMillis = maxFileModificationTime.toEpochMilli() - 10_000L;

    InMemoryPersistenceSpi storage = new InMemoryPersistenceSpi();
    UUID id = UUID.randomUUID();
    storage.startIdentifyLiveContents(id, now);
    storage.addIdentifiedLiveContent(
        id,
        Stream.of(
            icebergContent(
                ICEBERG_TABLE,
                CONTENT_ID,
                "12345678",
                ContentKey.of("foo", "bar"),
                BASE.resolve("metadata-1").toString(),
                42L)));
    storage.finishedIdentifyLiveContents(id, now, null);
    LiveContentSet liveContentSet = storage.getLiveContentSet(id);

    // Live files referenced by the content:
    // * "metadata-1" is a regular file under the content's base location
    // * "abs-live.parquet" is referenced via an absolute URI, but happens to be located under the
    //   base location, so the sweep phase sees it while listing
    // * "other.parquet" is referenced via an absolute URI outside the base location, it is never
    //   listed and must not cause expiry to fail
    ContentToFiles contentToFiles =
        contentReference ->
            Stream.of(
                FileReference.of(StorageUri.of("metadata-1"), BASE, -1L),
                FileReference.of(BASE.resolve("abs-live.parquet"), BASE, -1L),
                FileReference.of(StorageUri.of("mock://elsewhere/other.parquet"), BASE, -1L));

    FilesLister lister =
        path -> {
          soft.assertThat(path).isEqualTo(BASE);
          return Stream.of(
              FileReference.of(StorageUri.of("metadata-1"), path, oldMillis),
              FileReference.of(StorageUri.of("abs-live.parquet"), path, oldMillis),
              FileReference.of(StorageUri.of("unused-1"), path, oldMillis),
              FileReference.of(
                  StorageUri.of("too-new"), path, maxFileModificationTime.toEpochMilli() + 1));
        };

    Set<StorageUri> deleted = ConcurrentHashMap.newKeySet();
    FileDeleter deleter =
        fileObject -> {
          deleted.add(fileObject.absolutePath());
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
                    .contentToFiles(contentToFiles)
                    .maxFileModificationTime(maxFileModificationTime)
                    .build())
            .build();

    DeleteSummary deleteSummary = localExpire.expire();

    soft.assertThat(deleteSummary)
        .extracting(DeleteSummary::deleted, DeleteSummary::failures)
        .containsExactly(1L, 0L);
    soft.assertThat(deleted).containsExactly(BASE.resolve("unused-1"));

    try (Stream<StorageUri> baseLocations = liveContentSet.fetchBaseLocations(CONTENT_ID)) {
      soft.assertThat(baseLocations).containsExactly(BASE);
    }
  }
}
