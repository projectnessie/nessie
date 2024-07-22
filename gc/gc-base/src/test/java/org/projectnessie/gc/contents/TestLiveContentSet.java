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
package org.projectnessie.gc.contents;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLiveContentSet {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void deferredFileDeleter(@TempDir Path dir) throws Exception {
    PersistenceSpi persistenceSpi = new InMemoryPersistenceSpi();
    UUID id = UUID.randomUUID();
    persistenceSpi.startIdentifyLiveContents(id, Instant.now());
    persistenceSpi.finishedIdentifyLiveContents(id, Instant.now(), null);
    persistenceSpi.startExpireContents(id, Instant.now());
    LiveContentSet liveContentSet = persistenceSpi.getLiveContentSet(id);

    StorageUri base = StorageUri.of(dir.toUri());
    StorageUri path = StorageUri.of("foo");
    StorageUri path2 = StorageUri.of("foo2");
    StorageUri path3 = StorageUri.of("foo3");

    FileReference ref = FileReference.of(path, base, -1L);
    FileReference ref2 = FileReference.of(path2, base, -1L);
    FileReference ref3 = FileReference.of(path3, base, -1L);

    // Requires status EXPIRY_IN_PROGRESS to store file-deletions
    FileDeleter deferredFileDeleter = liveContentSet.fileDeleter();
    deferredFileDeleter.delete(ref);
    deferredFileDeleter.deleteMultiple(base, Stream.of(ref2, ref3));

    // Requires status EXPIRY_SUCCESS to fetch file-deletions
    persistenceSpi.finishedExpireContents(id, Instant.now(), null);

    try (Stream<FileReference> deletions = persistenceSpi.fetchFileDeletions(id)) {
      soft.assertThat(deletions).containsExactlyInAnyOrder(ref, ref2, ref3);
    }

    try (Stream<FileReference> deletions =
        persistenceSpi.getLiveContentSet(id).fetchFileDeletions()) {
      soft.assertThat(deletions).containsExactlyInAnyOrder(ref, ref2, ref3);
    }
  }
}
