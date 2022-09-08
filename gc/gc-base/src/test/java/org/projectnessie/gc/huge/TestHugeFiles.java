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
package org.projectnessie.gc.huge;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.BitSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.huge.HugeFiles.BasePath;
import org.projectnessie.model.ContentKey;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/** Tests {@link HugeFiles} (code for tests), not anything in the production code base. */
public class TestHugeFiles {

  @Test
  void pathParsing() {
    assertThat(BasePath.fromPath(URI.create("foo://bar/base/uri/path/contentId/3/5/hello-file")))
        .extracting(
            b -> b.retainedSnapshots, b -> b.filesPerSnapshot, b -> b.contentId, b -> b.base)
        .containsExactly(3, 5, "contentId", URI.create("foo://bar/base/uri/path/contentId/3/5/"));
  }

  @Test
  void checkFirstSnapshot() {
    URI base = URI.create("foo://bar/base/uri/path/contentId/3/5/");
    URI meta = base.resolve("hello.there");
    assertThat(meta.toString()).isEqualTo(base + "hello.there");
    assertThat(
            new HugeFiles()
                .extractFiles(
                    ContentReference.icebergTable(
                        "cid", "12345678", ContentKey.of("foo"), meta.toString(), 0)))
        .containsExactlyInAnyOrder(
            FileReference.of(URI.create("file-0-0.dummy"), base, 0L),
            FileReference.of(URI.create("file-0-1.dummy"), base, 1L),
            FileReference.of(URI.create("file-0-2.dummy"), base, 2L),
            FileReference.of(URI.create("file-0-3.dummy"), base, 3L),
            FileReference.of(URI.create("file-0-4.dummy"), base, 4L));
  }

  @Test
  void checkFirstThreeSnapshots() {
    URI base = URI.create("foo://bar/base/uri/path/contentId/3/5/");
    URI meta = base.resolve("hello.there");
    assertThat(meta.toString()).isEqualTo(base + "hello.there");
    assertThat(
            new HugeFiles()
                .extractFiles(
                    ContentReference.icebergTable(
                        "cid", "12345678", ContentKey.of("foo"), meta.toString(), 2)))
        .containsExactlyInAnyOrder(
            fileObject(base, 0, 0),
            fileObject(base, 0, 1),
            fileObject(base, 0, 2),
            fileObject(base, 0, 3),
            fileObject(base, 0, 4),
            //
            fileObject(base, 1, 0),
            fileObject(base, 1, 1),
            fileObject(base, 1, 2),
            fileObject(base, 1, 3),
            fileObject(base, 1, 4),
            //
            fileObject(base, 2, 0),
            fileObject(base, 2, 1),
            fileObject(base, 2, 2),
            fileObject(base, 2, 3),
            fileObject(base, 2, 4));
  }

  @Test
  void checkSomePreviousSnapshots() {
    URI base = URI.create("foo://bar/base/uri/path/contentId/3/5/");
    URI meta = base.resolve("hello.there");
    assertThat(meta.toString()).isEqualTo(base + "hello.there");
    assertThat(
            new HugeFiles()
                .extractFiles(
                    ContentReference.icebergTable(
                        "cid", "12345678", ContentKey.of("foo"), meta.toString(), 10)))
        .containsExactlyInAnyOrder(
            fileObject(base, 8, 0),
            fileObject(base, 8, 1),
            fileObject(base, 8, 2),
            fileObject(base, 8, 3),
            fileObject(base, 8, 4),
            //
            fileObject(base, 9, 0),
            fileObject(base, 9, 1),
            fileObject(base, 9, 2),
            fileObject(base, 9, 3),
            fileObject(base, 9, 4),
            //
            fileObject(base, 10, 0),
            fileObject(base, 10, 1),
            fileObject(base, 10, 2),
            fileObject(base, 10, 3),
            fileObject(base, 10, 4));
  }

  @Test
  void listing() throws Exception {
    HugeFiles files = new HugeFiles();

    URI base = URI.create("foo://bar/base/uri/path/contentId/3/5/");
    assertThat(files.listRecursively(base)).isEmpty();

    files.createSnapshot("contentId", 9);
    assertThat(files.listRecursively(base))
        .containsExactlyInAnyOrder(
            fileObject(base, 9, 0),
            fileObject(base, 9, 1),
            fileObject(base, 9, 2),
            fileObject(base, 9, 3),
            fileObject(base, 9, 4));
    assertThat(files.contents).containsOnlyKeys("contentId");
    assertThat(files.contents.get("contentId").availableSnapshots)
        .extracting(BitSet::cardinality, b -> b.get(9))
        .containsExactly(1, true);
    assertThat(files.contents.get("contentId").partialDeletions).isEmpty();

    for (int i = 0; i <= 10; i++) {
      files.createSnapshot("contentId", i);
    }
    assertThat(files.listRecursively(base))
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(0, 10)
                .boxed()
                .flatMap(
                    snap ->
                        IntStream.range(0, 5).mapToObj(fileNum -> fileObject(base, snap, fileNum)))
                .collect(Collectors.toList()));
    assertThat(files.contents.get("contentId").availableSnapshots)
        .extracting(BitSet::cardinality)
        .isEqualTo(11);

    // Delete 1 file in each snapshot
    for (int i = 0; i <= 10; i++) {
      files.delete(fileObject(base, i, 1));
    }
    assertThat(files.listRecursively(base))
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(0, 10)
                .boxed()
                .flatMap(
                    snap ->
                        IntStream.range(0, 5)
                            .filter(f -> f != 1)
                            .mapToObj(fileNum -> fileObject(base, snap, fileNum)))
                .collect(Collectors.toList()));
    assertThat(files.contents.get("contentId").partialDeletions)
        .containsOnlyKeys(IntStream.rangeClosed(0, 10).boxed().collect(Collectors.toSet()));

    // Delete all files of snapshots 4 + 6
    for (int i = 0; i < 5; i++) {
      files.delete(fileObject(base, 6, i));
      files.delete(fileObject(base, 4, i));
    }
    assertThat(files.contents.get("contentId").availableSnapshots.get(4)).isFalse();
    assertThat(files.contents.get("contentId").availableSnapshots.get(6)).isFalse();
    assertThat(files.contents.get("contentId").partialDeletions.get(4)).isNull();
    assertThat(files.contents.get("contentId").partialDeletions.get(6)).isNull();
    assertThat(files.contents.get("contentId").partialDeletions)
        .hasSize(9)
        .containsOnlyKeys(
            IntStream.rangeClosed(0, 10)
                .filter(s -> s != 4 && s != 6)
                .boxed()
                .collect(Collectors.toSet()));
    assertThat(files.listRecursively(base))
        .containsExactlyInAnyOrderElementsOf(
            IntStream.rangeClosed(0, 10)
                .filter(s -> s != 4 && s != 6)
                .boxed()
                .flatMap(
                    snap ->
                        IntStream.range(0, 5)
                            .filter(f -> f != 1)
                            .mapToObj(fileNum -> fileObject(base, snap, fileNum)))
                .collect(Collectors.toList()));
  }

  private static FileReference fileObject(URI base, int snapshotId, int fileNum) {
    return FileReference.of(
        URI.create("file-" + snapshotId + "-" + fileNum + ".dummy"),
        base,
        snapshotId * 1_000_000_000L + fileNum);
  }

  @Test
  void manyFiles() {
    URI base = URI.create("foo://bar/base/uri/path/contentId/10/400000/");
    URI meta = base.resolve("hello.there");
    assertThat(meta.toString()).isEqualTo(base + "hello.there");
    assertThat(
            new HugeFiles()
                .extractFiles(
                    ContentReference.icebergTable(
                        "cid", "12345678", ContentKey.of("foo"), meta.toString(), 1000))
                .count())
        .isEqualTo(4_000_000L);
  }
}
