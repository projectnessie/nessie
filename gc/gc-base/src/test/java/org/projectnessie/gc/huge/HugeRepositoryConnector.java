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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableLogEntry;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/**
 * Produces a number of references, commits and {@link Operation.Put} operations with a number of
 * different shared and unshared contents, without maintaining any state.
 *
 * <p>This class simulates a Nessie repository with a "main" branch plus {@link #refCount}
 * "additional" branches. "Additional" branch are "created" off of the "main" branch every {@link
 * #commitMultiplier} commits, so the first "additional" branch after {@link #commitMultiplier}
 * commits on "main", the second "additional" branch after {@link #commitMultiplier} more commits on
 * "main" and so on. Each "additional" branch has {@link #commitMultiplier} commits.
 *
 * <p>There are three "categories" of contents: content that has {@link Operation.Put} operations
 * only on "main" (with content-IDs starting with {@value #CID_MAIN}), content that has {@link
 * Operation.Put} operations on "main" and "additional" branches (with content-IDs starting with
 * {@value #CID_SHARED}) and content that has {@link Operation.Put} operations only on "additional"
 * branches (with content-IDs starting with {@value #CID_EXCLUSIVE}).
 *
 * <p>The commits that have {@link Operation.Put} operations for a specific content-ID are
 * determined using some math: First, the number of <em>possible</em> contents of a branch is
 * determined: {@link #totalContentsOnMain()} for the "main" branch and {@link
 * #contentsOnReference()} for "additional" branches. Roughly speaking: the content-ID is determined
 * by a modulo operation using the commit number.
 *
 * <p>This class uses <em>special</em> commit-IDs that have the branch and commit number encoded,
 * see {@link #commitId(int, int)}.
 */
final class HugeRepositoryConnector implements RepositoryConnector {

  static final String CID_MAIN = "main-";
  static final String CID_SHARED = "shared-";
  static final String CID_EXCLUSIVE = "exclusive-";
  static final String ADDITIONAL_BRANCH = "branch-";
  static final String MAIN_BRANCH = "main";

  final int refCount;
  final int commitMultiplier;

  final int retainedSnapshots;
  final int filesPerSnapshot;

  final int contentsMainExclusive = 20;
  final int contentsShared = 2;
  final int contentsReferenceExclusive = 2;

  /*
   * Contents to branch mapping:
   *
   * - "main" branch: 20 contents exclusive for main
   * - 2 contents per reference - originate from main
   * - 2 contents per reference
   *
   * Content-Num mapping:
   * 0 .. 19 --> main
   * 20 .. (refCount * 2) --> main + reference
   * (20 + refCount * 2) .. (20 + refCount * 4) --> reference
   */

  HugeRepositoryConnector(
      int refCount, int commitMultiplier, int retainedSnapshots, int filesPerSnapshot) {
    this.refCount = refCount;
    this.commitMultiplier = commitMultiplier;
    this.retainedSnapshots = retainedSnapshots;
    this.filesPerSnapshot = filesPerSnapshot;
  }

  /** Returns a pseudo commit-ID with the reference number and commit number encoded. */
  String commitId(int refNum, int num) {
    int refOffset = commitMultiplier * refNum;
    if (num <= refOffset) {
      // commit is on "main" branch
      refNum = 0;
    }
    StringBuilder id = new StringBuilder("0000000000000000");
    for (int i = 7, v = refNum; i >= 0; i--, v >>= 4) {
      id.setCharAt(i, HEX[v & 0xf]);
    }
    for (int i = 15, v = num; i >= 8; i--, v >>= 4) {
      id.setCharAt(i, HEX[v & 0xf]);
    }
    return id.toString();
  }

  static final char[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  private IcebergTable table(String contentId, int snapshotId) {
    return IcebergTable.of(metadataLocation(contentId, snapshotId), snapshotId, 0, 0, 0, contentId);
  }

  String metadataLocation(String contentId, int snapshotId) {
    return "mock://foo/path/"
        + contentId
        + "/"
        + retainedSnapshots
        + "/"
        + filesPerSnapshot
        + "/meta-"
        + snapshotId
        + ".dummy";
  }

  static ContentKey contentKey(String contentId) {
    return ContentKey.of(contentId);
  }

  static int referenceFromCommitId(String commitId) {
    return Integer.parseInt(commitId.substring(0, 8), 16);
  }

  static int commitFromCommitId(String commitId) {
    return Integer.parseInt(commitId.substring(8), 16);
  }

  @Override
  public Stream<Reference> allReferences() {
    return IntStream.rangeClosed(0, refCount)
        .mapToObj(
            refNum ->
                refNum == 0
                    ? Branch.of(MAIN_BRANCH, commitId(0, totalCommitsOnMain()))
                    : Branch.of(
                        ADDITIONAL_BRANCH + refNum,
                        commitId(refNum, commitMultiplier + commitMultiplier * refNum)));
  }

  int totalCommitsOnMain() {
    return 2 * commitMultiplier + commitMultiplier * refCount;
  }

  int totalContentsOnMain() {
    return contentsMainExclusive + contentsShared * refCount;
  }

  int contentsOnReference() {
    return contentsShared + contentsReferenceExclusive;
  }

  int tippingPoint(int refNum) {
    return refNum * commitMultiplier;
  }

  String contentIdForCommit(int refNum, int commitNum) {
    if (refNum == 0) {
      int c = commitNum % totalContentsOnMain();
      if (c < contentsMainExclusive) {
        return CID_MAIN + c;
      }

      int c2 = c - contentsMainExclusive;
      int ref = c2 / contentsShared + 1;

      if (commitNum > tippingPoint(ref)) {
        // post branch creation commit on "main" (not changed on "main" post this commit)
        return null;
      }

      return CID_SHARED + ref + "-" + (c2 % contentsShared);
    }

    int commitNumOnRef = commitNum - tippingPoint(refNum);
    int c = commitNumOnRef % contentsOnReference();
    if (c < contentsShared) {
      // content shared with main
      return CID_SHARED + refNum + "-" + c;
    } else {
      // content exclusive on reference
      return CID_EXCLUSIVE + refNum + "-" + (c - contentsShared);
    }
  }

  int modCountForCommit(int refNum, int commitNum) {
    int tcMain = totalContentsOnMain();

    if (refNum == 0) {
      int c = commitNum % tcMain;
      if (c < contentsMainExclusive) {
        return commitNum / tcMain;
      }

      int c2 = c - contentsMainExclusive;
      int ref = c2 / contentsShared + 1;

      if (commitNum > tippingPoint(ref)) {
        // post branch creation commit on "main" (not changed on "main" post this commit)
        throw new IllegalArgumentException();
      }

      return commitNum / tcMain;
    }

    int tp = tippingPoint(refNum);
    int commitNumOnRef = commitNum - tp;
    int contentsOnRef = contentsOnReference();
    int c = commitNumOnRef % contentsOnRef;
    int changesOnRef = commitNumOnRef / contentsOnRef;
    if (c < contentsShared) {
      int changesOnMain = tp / tcMain;
      return changesOnMain + changesOnRef;
    } else {
      return changesOnRef;
    }
  }

  @Override
  public Stream<LogEntry> commitLog(Reference ref) {
    int refNum = referenceFromCommitId(ref.getHash());
    int commitNum = commitFromCommitId(ref.getHash());
    int refTp = tippingPoint(refNum);

    LongToIntFunction commitToReference = c -> c > refTp ? refNum : 0;
    IntFunction<String> commitToId = c -> commitId(commitToReference.applyAsInt(c), c);

    return IntStream.range(0, commitNum)
        .map(c -> commitNum - c)
        .mapToObj(
            c -> {
              String commitId = commitToId.apply(c);
              ImmutableLogEntry.Builder b =
                  LogEntry.builder()
                      .commitMeta(
                          CommitMeta.builder()
                              .message("Commit #" + commitNum)
                              .hash(commitId)
                              .commitTime(Instant.ofEpochMilli(c * 1000L))
                              .build())
                      .parentCommitHash(commitToId.apply(c - 1));

              int commitRefNum = commitToReference.applyAsInt(c);
              String contentId = contentIdForCommit(commitRefNum, c);

              if (contentId != null) {
                int snapshotId = modCountForCommit(commitRefNum, c);

                b.addOperations(
                    Operation.Put.of(contentKey(contentId), table(contentId, snapshotId)));
              }

              return b.build();
            });
  }

  @Override
  public Stream<Entry<ContentKey, Content>> allContents(Detached ref, Set<Content.Type> types) {
    if (!types.contains(Content.Type.ICEBERG_TABLE)) {
      return Stream.empty();
    }

    int refNum = referenceFromCommitId(ref.getHash());
    int commitNum = commitFromCommitId(ref.getHash());

    Map<ContentKey, Content> contents = new HashMap<>();

    int commitOnMain;
    if (refNum > 0) {
      for (int c = commitNum, i = contentsOnReference(); i > 0; i--, c--) {
        String contentId = contentIdForCommit(refNum, c);
        int snapshotId = modCountForCommit(refNum, c);
        contents.putIfAbsent(contentKey(contentId), table(contentId, snapshotId));
      }

      commitOnMain = tippingPoint(refNum);
    } else {
      commitOnMain = commitNum;
    }

    int tcMain = totalContentsOnMain();

    for (int c = commitOnMain, i = tcMain; i > 0 && c > 0; i--, c--) {
      String contentId = contentIdForCommit(0, c);
      if (contentId != null) {
        int commit = c;
        contents.computeIfAbsent(
            contentKey(contentId), x -> table(contentId, modCountForCommit(0, commit)));
      }
    }

    for (int r = 1; r <= refCount; r++) {
      int max = tippingPoint(r);
      if (commitNum > max) {
        for (int i = 0; i < contentsShared; i++) {
          String contentId = CID_SHARED + r + "-" + i;
          contents.computeIfAbsent(contentKey(contentId), x -> table(contentId, max / tcMain));
        }
      }
    }

    return contents.entrySet().stream();
  }

  @Override
  public void close() {}
}
