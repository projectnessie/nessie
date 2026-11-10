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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet.Status;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.model.ContentKey;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Verifies that expiry does not delete files in base locations that are shared by multiple
 * contents, see <a href="https://github.com/projectnessie/nessie/issues/12363">issue #12363</a>.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestExpireSharedBaseLocations {
  @InjectSoftAssertions SoftAssertions soft;

  final Instant now = Instant.now();
  final long oldMillis = now.minus(1, ChronoUnit.HOURS).toEpochMilli();

  final InMemoryPersistenceSpi storage = new InMemoryPersistenceSpi();
  final UUID id = UUID.randomUUID();
  final List<ContentReference> contentReferences = new ArrayList<>();

  /** All existing files. */
  final Set<StorageUri> files = ConcurrentHashMap.newKeySet();

  /** Adds a content with one content reference per metadata file and one unreferenced file. */
  void content(String contentId, String baseLocation, String... metadataFiles) {
    for (String metadataFile : metadataFiles) {
      contentReferences.add(
          icebergContent(
              ICEBERG_TABLE,
              contentId,
              "12345678",
              ContentKey.of(contentId),
              baseLocation + metadataFile,
              contentReferences.size()));
      files.add(StorageUri.of(baseLocation + metadataFile));
    }
    files.add(StorageUri.of(baseLocation + "orphan-" + contentId));
  }

  /** The metadata file is the only file of a content reference, located in the base location. */
  final ContentToFiles contentToFiles =
      new ContentToFiles() {
        @Override
        public Stream<FileReference> extractFiles(ContentReference ref) {
          StorageUri metadata = StorageUri.of(ref.metadataLocation());
          return extractBaseLocations(ref)
              .map(base -> FileReference.of(base.relativize(metadata), base, -1L));
        }

        @Override
        public Stream<StorageUri> extractBaseLocations(ContentReference ref) {
          String metadata = ref.metadataLocation();
          return Stream.of(StorageUri.of(metadata.substring(0, metadata.lastIndexOf('/') + 1)));
        }
      };

  void expire(ContentToFiles contentToFiles) throws Exception {
    storage.startIdentifyLiveContents(id, now);
    storage.addIdentifiedLiveContent(id, contentReferences.stream());
    storage.finishedIdentifyLiveContents(id, now, null);

    DefaultLocalExpire.builder()
        .expireParameters(
            ExpireParameters.builder()
                .liveContentSet(storage.getLiveContentSet(id))
                .filesLister(base ->
                    files.stream()
                        .filter(f -> f.location().startsWith(base.location()))
                        .map(f -> FileReference.of(base.relativize(f), base, oldMillis))
                        .toList()
                        .stream())
                .fileDeleter(f -> {
                  files.remove(f.absolutePath());
                  return DeleteResult.SUCCESS;
                })
                .expectedFileCount(100)
                .contentToFiles(contentToFiles)
                .maxFileModificationTime(now)
                .build())
        .build()
        .expire();
  }

  Stream<String> orphans() {
    return files.stream().map(StorageUri::location).filter(l -> l.contains("orphan-"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void sharedBaseLocationsAreNotExpired(boolean defaultExtractBaseLocations)
      throws Exception {
    // table dropped and re-created at the same location
    content("old", "mock://bucket/shared/", "old-1.json", "old-2.json");
    content("new", "mock://bucket/shared/", "new-1.json");
    // nested table locations
    content("outer", "mock://bucket/nested/", "outer-1.json");
    content("inner", "mock://bucket/nested/inner/", "inner-1.json");
    // tables with their own locations
    content("t", "mock://bucket/t/", "t-1.json");
    content("t_1", "mock://bucket/t_1/", "t_1-1.json");
    int filesBefore = files.size();

    expire(defaultExtractBaseLocations ? ref -> contentToFiles.extractFiles(ref) : contentToFiles);

    // only the unreferenced files of 't' and 't_1' are deleted
    soft.assertThat(files).hasSize(filesBefore - 2);
    soft.assertThat(orphans())
        .containsExactlyInAnyOrder(
            "mock://bucket/shared/orphan-old",
            "mock://bucket/shared/orphan-new",
            "mock://bucket/nested/orphan-outer",
            "mock://bucket/nested/inner/orphan-inner");
    soft.assertThat(storage.getLiveContentSet(id).status()).isEqualTo(Status.EXPIRY_SUCCESS);
  }
}
