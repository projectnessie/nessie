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

import java.net.URI;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.agrona.collections.Int2ObjectHashMap;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.expire.ContentToFiles;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/**
 * Provides file related operations for {@link TestManyObjects}, supporting recursive file {@link
 * FilesLister listing}, {@link FileDeleter deletion} and {@link ContentToFiles content-to-files}.
 *
 * <p>Generates {@link Stream}s producing a lot of {@link FileReference}s for an imaginary {@link
 * ContentReference}.
 *
 * <p>The "age" of a referenced content is determined by its {@link ContentReference#snapshotId()
 * snapshot ID}: The files of up to "N" "previous" snapshots are also included. The number of "N" is
 * determined via the {@link ContentReference#metadataLocation()}, as well as the number of file
 * objects per snapshot.
 *
 * <p>Information needed to generate the files for a content reference, list + delete files is
 * encoded in the base-path URI. To file deletions and "synchronize" with file listing, this class
 * maintains {@link PerContent some state} to track the available snapshots/files per content ID.
 */
final class HugeFiles implements ContentToFiles, FilesLister, FileDeleter {

  static class BasePath {
    final int retainedSnapshots;
    final int filesPerSnapshot;
    final String contentId;
    final URI base;

    private BasePath(int retainedSnapshots, int filesPerSnapshot, String contentId, URI base) {
      this.retainedSnapshots = retainedSnapshots;
      this.filesPerSnapshot = filesPerSnapshot;
      this.contentId = contentId;
      this.base = base;
    }

    static BasePath fromContentReference(ContentReference contentReference) {
      URI meta = URI.create(contentReference.metadataLocation());
      return fromPath(meta);
    }

    static BasePath fromPath(URI path) {
      String metaPath = path.getPath();
      int last = metaPath.lastIndexOf('/');
      int last2 = metaPath.lastIndexOf('/', last - 1);
      int last3 = metaPath.lastIndexOf('/', last2 - 1);
      int last4 = metaPath.lastIndexOf('/', last3 - 1);
      String contentId = metaPath.substring(last4 + 1, last3);
      int maxSnapshots = Integer.parseInt(metaPath.substring(last3 + 1, last2));
      int filesPerSnapshot = Integer.parseInt(metaPath.substring(last2 + 1, last));
      String basePath = metaPath.substring(0, last + 1);
      URI base = path.resolve(basePath);

      return new BasePath(maxSnapshots, filesPerSnapshot, contentId, base);
    }
  }

  @Override
  public Stream<FileReference> extractFiles(ContentReference contentReference) {
    BasePath base = BasePath.fromContentReference(contentReference);

    return LongStream.rangeClosed(
            Math.max(0L, contentReference.snapshotId() - base.retainedSnapshots + 1),
            contentReference.snapshotId())
        .boxed()
        .flatMap(
            snapshotId ->
                IntStream.range(0, base.filesPerSnapshot)
                    .mapToObj(
                        fileNum -> {
                          URI path = URI.create("file-" + snapshotId + "-" + fileNum + ".dummy");
                          long modificationTimestamp = snapshotId * 1_000_000_000L + fileNum;
                          return FileReference.of(path, base.base, modificationTimestamp);
                        }));
  }

  static final class PerContent {
    final BitSet availableSnapshots = new BitSet();
    final Int2ObjectHashMap<BitSet> partialDeletions = new Int2ObjectHashMap<>();
  }

  final Map<String, PerContent> contents = new ConcurrentHashMap<>();

  @Override
  public Stream<FileReference> listRecursively(URI path) {
    BasePath base = BasePath.fromPath(path);
    PerContent perContent = contents.get(base.contentId);
    if (perContent == null) {
      return Stream.empty();
    }

    return IntStream.range(0, perContent.availableSnapshots.size())
        .filter(perContent.availableSnapshots::get)
        .boxed()
        .flatMap(
            snapshotId -> {
              BitSet partialDeletions = perContent.partialDeletions.get(snapshotId);
              return IntStream.range(0, base.filesPerSnapshot)
                  .filter(f -> partialDeletions == null || !partialDeletions.get(f))
                  .mapToObj(
                      fileNum -> {
                        URI file = URI.create("file-" + snapshotId + "-" + fileNum + ".dummy");
                        long modificationTimestamp = snapshotId * 1_000_000_000L + fileNum;
                        return FileReference.of(file, base.base, modificationTimestamp);
                      });
            });
  }

  @Override
  public DeleteResult delete(FileReference fileReference) {
    BasePath base = BasePath.fromPath(fileReference.base());
    PerContent perContent = contents.get(base.contentId);
    if (perContent == null) {
      return DeleteResult.SUCCESS;
    }

    String filePath = fileReference.path().getPath();
    int dash = filePath.lastIndexOf('-');
    int dash2 = filePath.lastIndexOf('-', dash - 1);
    int dot = filePath.lastIndexOf('.');
    int fileNum = Integer.parseInt(filePath.substring(dash + 1, dot));
    int snapshotId = Integer.parseInt(filePath.substring(dash2 + 1, dash));

    if (!perContent.availableSnapshots.get(snapshotId)) {
      return DeleteResult.SUCCESS;
    }

    BitSet partials = perContent.partialDeletions.computeIfAbsent(snapshotId, i -> new BitSet());
    partials.set(fileNum);

    if (partials.cardinality() == base.filesPerSnapshot) {
      perContent.partialDeletions.remove(snapshotId);
      perContent.availableSnapshots.clear(snapshotId);
    }

    return DeleteResult.SUCCESS;
  }

  public void createSnapshot(String contentId, int snapshotId) {
    PerContent perContent = contents.computeIfAbsent(contentId, x -> new PerContent());
    perContent.availableSnapshots.set(snapshotId);
  }
}
