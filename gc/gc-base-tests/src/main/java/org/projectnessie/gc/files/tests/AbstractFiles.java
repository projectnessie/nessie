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
package org.projectnessie.gc.files.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.storage.uri.StorageUri;

public abstract class AbstractFiles {

  @Test
  public void walkEmpty() throws Exception {
    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files).isEmpty();
    }
  }

  @Test
  public void deleteNonExisting() {
    assertThat(deleter().delete(FileReference.of(StorageUri.of("fileX"), baseUri(), 123L)))
        .isEqualTo(DeleteResult.SUCCESS);
  }

  @Test
  public void walkSomeFilesAlt() throws Exception {
    List<FileReference> fileList = prepareFiles(5);

    Set<FileReference> expect = new HashSet<>(fileList);

    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files)
          .allSatisfy(f -> assertThat(f.base()).isEqualTo(baseUri()))
          .containsExactlyInAnyOrderElementsOf(expect);
    }

    FileReference f2 = fileList.get(1);
    FileReference f3 = fileList.get(2);
    FileReference f4 = fileList.get(3);

    deleter().deleteMultiple(baseUri(), Stream.of(f2, f3));
    expect.remove(f2);
    expect.remove(f3);

    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files)
          .allSatisfy(f -> assertThat(f.base()).isEqualTo(baseUri()))
          .containsExactlyInAnyOrderElementsOf(expect);
    }

    deleter().delete(f4);
    expect.remove(f4);

    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files)
          .allSatisfy(f -> assertThat(f.base()).isEqualTo(baseUri()))
          .containsExactlyInAnyOrderElementsOf(expect);
    }
  }

  @Test
  public void walkSomeFiles() throws Exception {
    List<FileReference> fileList = prepareFiles(5);

    Set<FileReference> expect = new HashSet<>(fileList);

    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files).containsExactlyInAnyOrderElementsOf(expect);
    }

    FileReference f5 = fileList.get(4);
    assertThat(deleter().delete(f5)).isEqualTo(DeleteResult.SUCCESS);
    expect.remove(f5);
    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files).containsExactlyInAnyOrderElementsOf(expect);
    }

    FileReference f2 = fileList.get(1);
    assertThat(deleter().delete(f2)).isEqualTo(DeleteResult.SUCCESS);
    expect.remove(f2);
    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files).containsExactlyInAnyOrderElementsOf(expect);
    }
  }

  /**
   * Creates many files, lists the files, deletes 10% of the created files, lists again.
   *
   * <p>Note for Minio: the configuration used for tests is not particularly fast - creating 100000
   * objects with 4 threads (more crashes w/ timeouts) takes about ~30 minutes (plus ~3 seconds for
   * listing 100000 objects, plus ~3 seconds for deleting 10000 objects).
   */
  @Test
  public void manyFiles() throws Exception {
    int numFiles = 500;

    List<FileReference> fileList = prepareFiles(numFiles);

    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files).containsExactlyInAnyOrderElementsOf(fileList);
    }

    int deletes = numFiles / 10;
    assertThat(
            deleter()
                .deleteMultiple(
                    baseUri(),
                    IntStream.rangeClosed(1, deletes)
                        .mapToObj(i -> StorageUri.of(dirAndFilename(i)))
                        .map(p -> FileReference.of(p, baseUri(), -1L))))
        .isEqualTo(DeleteSummary.of(deletes, 0L));

    try (Stream<FileReference> files = lister().listRecursively(baseUri())) {
      assertThat(files).hasSize(numFiles - deletes);
    }
  }

  protected abstract StorageUri baseUri();

  protected abstract FilesLister lister();

  protected abstract FileDeleter deleter();

  protected abstract List<FileReference> prepareFiles(int numFiles);

  protected List<FileReference> prepareLocalFiles(Path path, int numFiles) {
    List<FileReference> r = new ArrayList<>(numFiles);
    for (int i = 1; i <= numFiles; i++) {
      try {
        Path f = path.resolve(dirAndFilename(i));
        Files.createDirectories(f.getParent());
        Files.createFile(f);
        FileReference fileReference =
            FileReference.of(
                baseUri().relativize(StorageUri.of(f.toUri())),
                baseUri(),
                Files.getLastModifiedTime(f).toMillis());
        r.add(fileReference);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return r;
  }

  protected static String dirAndFilename(int i) {
    return String.format("dir-%d/file-%d", i % 100, i);
  }
}
