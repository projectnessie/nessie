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
package org.projectnessie.versioned.transfer;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class BaseExportImport {
  @InjectSoftAssertions protected SoftAssertions soft;

  @TempDir Path dir;

  abstract Persist sourcePersist();

  abstract Persist targetPersist();

  abstract VersionStore sourceVersionStore();

  abstract VersionStore targetVersionStore();

  abstract void prepareTargetRepo();

  abstract ImportResult importRepo(boolean zip) throws IOException;

  abstract ExportMeta exportRepo(boolean zip, boolean fullScan) throws Exception;

  @MustBeClosed
  abstract Stream<Hash> scanAllTargetCommits();

  public interface VersionStoreSetup {
    void setup(
        VersionStore vs,
        Persist persist,
        HeadsAndForks.Builder headsANdForks,
        Consumer<ByteString> deleted)
        throws Exception;
  }

  static Stream<Arguments> scenarios() {
    BranchName mainBranch = BranchName.of("main");

    return Stream.of(
        // Scenario: pretty empty
        arguments(
            0, // commitsTotal
            0, // commitsLive
            1, // namedRefs
            0, // generic objects
            0, // generic objects
            (VersionStoreSetup) (vs, persist, headsAndForks, deleted) -> {}),
        // Scenario: branch + tag + default branch
        arguments(
            0, // commitsTotal
            0, // commitsLive
            3, // namedRefs
            0, // generic objects
            0, // generic objects
            (VersionStoreSetup)
                (vs, persist, headsAndForks, deleted) -> {
                  vs.create(BranchName.of("branch"), Optional.of(vs.noAncestorHash()));
                  vs.create(TagName.of("tag"), Optional.of(vs.noAncestorHash()));

                  persist.storeObj(
                      UniqueIdObj.uniqueId("space", ByteString.copyFromUtf8("unique")));
                  persist.storeObj(
                      UniqueIdObj.uniqueId("other_space", ByteString.copyFromUtf8("also unique")));
                }),
        // 3 "independent" branches
        arguments(
            30, // commitsTotal
            30, // commitsLive
            3, // namedRefs
            30, // generic objects (unique-id objects)
            30, // generic objects
            (VersionStoreSetup)
                (vs, persist, headsAndForks, deleted) -> {
                  Hash main = vs.getNamedRef("main", GetNamedRefsParams.DEFAULT).getHash();
                  main = commit10(vs, 0, mainBranch, main);

                  BranchName branch = BranchName.of("a");
                  Hash a = vs.create(branch, Optional.of(vs.noAncestorHash())).getHash();
                  a = commit10(vs, 0, branch, a);

                  branch = BranchName.of("b");
                  Hash b = vs.create(branch, Optional.of(vs.noAncestorHash())).getHash();
                  b = commit10(vs, 0, branch, b);

                  headsAndForks.addHeads(main.asBytes());
                  headsAndForks.addHeads(a.asBytes());
                  headsAndForks.addHeads(b.asBytes());
                }),
        // 2 deleted branches (same as above)
        arguments(
            30, // commitsTotal
            10, // commitsLive
            1, // namedRefs
            30, // generic objects (unique-id objects)
            10, // generic objects
            (VersionStoreSetup)
                (vs, persist, headsAndForks, deleted) -> {
                  Hash main = vs.getNamedRef("main", GetNamedRefsParams.DEFAULT).getHash();
                  main = commit10(vs, 0, mainBranch, main);

                  BranchName branch = BranchName.of("a");
                  Hash a = vs.create(branch, Optional.of(vs.noAncestorHash())).getHash();
                  a = commit10(vs, 0, branch, a);

                  branch = BranchName.of("b");
                  Hash b = vs.create(branch, Optional.of(vs.noAncestorHash())).getHash();
                  b = commit10(vs, 0, branch, b);

                  vs.delete(BranchName.of("a"), a);
                  vs.delete(BranchName.of("b"), b);

                  headsAndForks.addHeads(a.asBytes());
                  deleted.accept(a.asBytes());
                  headsAndForks.addHeads(b.asBytes());
                  deleted.accept(b.asBytes());
                  headsAndForks.addHeads(main.asBytes());
                }),
        // 3 "independent" branches
        arguments(
            50, // commitsTotal
            50, // commitsLive
            3, // namedRefs
            50, // generic objects (unique-id objects)
            50, // generic objects
            (VersionStoreSetup)
                (vs, persist, headsAndForks, deleted) -> {
                  Hash main = vs.getNamedRef("main", GetNamedRefsParams.DEFAULT).getHash();
                  main = commit10(vs, 0, mainBranch, main);

                  BranchName branch = BranchName.of("a");
                  Hash a = vs.create(branch, Optional.of(main)).getHash();
                  headsAndForks.addForkPoints(main.asBytes());
                  a = commit10(vs, 0, branch, a);
                  main = commit10(vs, 10, mainBranch, main);

                  branch = BranchName.of("b");
                  Hash b = vs.create(branch, Optional.of(a)).getHash();
                  headsAndForks.addForkPoints(a.asBytes());
                  b = commit10(vs, 0, branch, b);
                  a = commit10(vs, 10, BranchName.of("a"), a);

                  headsAndForks.addHeads(main.asBytes());
                  headsAndForks.addHeads(a.asBytes());
                  headsAndForks.addHeads(b.asBytes());
                }),
        // merge
        arguments(
            41, // commitsTotal
            41, // commitsLive
            2, // namedRefs
            40, // generic objects (unique-id objects)
            40, // generic objects (commit log walking)
            (VersionStoreSetup)
                (vs, persist, headsAndForks, deleted) -> {
                  Hash main = vs.getNamedRef("main", GetNamedRefsParams.DEFAULT).getHash();
                  main = commit10(vs, 0, mainBranch, main);

                  BranchName branch = BranchName.of("a");
                  Hash a = vs.create(branch, Optional.of(main)).getHash();

                  headsAndForks.addForkPoints(main.asBytes());
                  a = commit10(vs, 0, branch, a);
                  main = commit10(vs, 10, mainBranch, main);

                  vs.merge(
                      MergeOp.builder()
                          .fromRef(branch)
                          .fromHash(a)
                          .toBranch(mainBranch)
                          .expectedHash(Optional.of(main))
                          .build());

                  main = commit10(vs, 20, mainBranch, main);

                  headsAndForks.addHeads(main.asBytes());
                  headsAndForks.addHeads(a.asBytes());
                }));
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("scenarios")
  public void scenariosFullScan(
      long commitsTotal,
      long commitsLive,
      long namedRefs,
      long generics,
      long genericsWalking,
      VersionStoreSetup setup)
      throws Exception {
    scenario(commitsTotal, namedRefs, generics, setup, false, true);
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("scenarios")
  public void scenariosFullScanZip(
      long commitsTotal,
      long commitsLive,
      long namedRefs,
      long generics,
      long genericsWalking,
      VersionStoreSetup setup)
      throws Exception {
    scenario(commitsTotal, namedRefs, generics, setup, true, true);
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("scenarios")
  public void scenariosCommitLogWalking(
      long commitsTotal,
      long commitsLive,
      long namedRefs,
      long generics,
      long genericsWalking,
      VersionStoreSetup setup)
      throws Exception {
    scenario(commitsLive, namedRefs, genericsWalking, setup, false, false);
  }

  private void scenario(
      long commits,
      long namedRefs,
      long generics,
      VersionStoreSetup setup,
      boolean zip,
      boolean fullScan)
      throws Exception {
    HeadsAndForks.Builder headsAndForksBuilder = HeadsAndForks.newBuilder();
    Set<ByteString> deletedHeads = new HashSet<>();
    setup.setup(sourceVersionStore(), sourcePersist(), headsAndForksBuilder, deletedHeads::add);
    HeadsAndForks headsAndForks = headsAndForksBuilder.build();

    ExportMeta exportMeta = exportRepo(zip, fullScan);
    soft.assertThat(exportMeta)
        .extracting(
            ExportMeta::getCommitCount,
            ExportMeta::getNamedReferencesCount,
            ExportMeta::getGenericObjCount,
            ExportMeta::getVersion)
        .containsExactly(commits, namedRefs, generics, exportVersion());

    prepareTargetRepo();

    ImportResult importResult = importRepo(zip);

    checkRepositoryDescription();

    List<ReferenceInfo<CommitMeta>> sourceNamedRefs = namedRefs(sourceVersionStore());
    List<ReferenceInfo<CommitMeta>> targetNamedRefs = namedRefs(targetVersionStore());
    soft.assertThat(targetNamedRefs).containsExactlyInAnyOrderElementsOf(sourceNamedRefs);

    for (ByteString bytes : headsAndForks.getHeadsList()) {
      Hash hash = Hash.of(bytes);
      List<Commit> sourceCommits = commits(sourceVersionStore(), hash);
      if (!fullScan && deletedHeads.contains(bytes)) {
        // The "head" exists in the source repo, but the ref has been deleted. Commit-log-export
        // only export named-references, so this head must not be present in the target repo.
        soft.assertThat(sourceCommits).isNotEmpty();
        soft.assertThatThrownBy(() -> commits(targetVersionStore(), hash))
            .isInstanceOf(ReferenceNotFoundException.class);

      } else {
        List<Commit> targetCommits = commits(targetVersionStore(), hash);
        soft.assertThat(targetCommits)
            .describedAs(hash.toString())
            .containsExactlyElementsOf(sourceCommits);
      }
    }

    try (Stream<Hash> scan = scanAllTargetCommits()) {
      scan.forEach(
          c -> {
            List<KeyEntry> sourceKeyEntries = keys(sourceVersionStore(), c);
            List<KeyEntry> targetKeyEntries = keys(targetVersionStore(), c);
            soft.assertThat(targetKeyEntries).containsExactlyInAnyOrderElementsOf(sourceKeyEntries);
            Set<ContentKey> keys =
                targetKeyEntries.stream()
                    .map(e -> e.getKey().contentKey())
                    .collect(Collectors.toSet());
            try {
              Map<ContentKey, ContentResult> targetValues =
                  targetVersionStore().getValues(c, keys, false);
              Map<ContentKey, ContentResult> sourceValues =
                  sourceVersionStore().getValues(c, keys, false);
              soft.assertThat(targetValues).containsExactlyInAnyOrderEntriesOf(sourceValues);
            } catch (ReferenceNotFoundException e) {
              throw new RuntimeException(e);
            }
          });
    }

    soft.assertThat(importResult)
        .extracting(
            ImportResult::importedCommitCount,
            ImportResult::importedReferenceCount,
            ImportResult::importedGenericCount,
            ImportResult::exportMeta)
        .containsExactly(commits, namedRefs, generics, exportMeta);
    List<Hash> importHeads = asHashes(importResult.headsAndForks().getHeadsList());
    List<Hash> expectHeads = asHashes(headsAndForks.getHeadsList());
    if (!fullScan) {
      deletedHeads.forEach(del -> expectHeads.remove(Hash.of(del)));
    }
    soft.assertThat(importHeads).containsExactlyInAnyOrderElementsOf(expectHeads);
    List<Hash> importForkPoints = asHashes(importResult.headsAndForks().getForkPointsList());
    List<Hash> expectForkPoints = asHashes(headsAndForks.getForkPointsList());
    soft.assertThat(importForkPoints).containsExactlyInAnyOrderElementsOf(expectForkPoints);
  }

  protected abstract void checkRepositoryDescription();

  static List<Hash> asHashes(List<ByteString> ids) {
    return ids.stream().map(Hash::of).collect(Collectors.toList());
  }

  abstract ExportVersion exportVersion();

  private static Hash commit10(VersionStore versionStore, int offset, BranchName branch, Hash head)
      throws Exception {
    for (int i = 0; i < 10; i++) {
      int commit = offset + i;
      head =
          versionStore
              .commit(
                  branch,
                  Optional.of(head),
                  CommitMeta.fromMessage("commit #" + commit + " " + branch),
                  Collections.singletonList(
                      Put.of(
                          ContentKey.of(branch.getName() + "-c-" + commit),
                          IcebergTable.of(
                              "meta+" + branch.getName() + "-c-" + commit + "-" + head.asString(),
                              42,
                              43,
                              44,
                              45))))
              .getCommitHash();
    }
    return head;
  }

  List<KeyEntry> keys(VersionStore versionStore, Hash hash) {
    try (PaginationIterator<KeyEntry> keys =
        versionStore.getKeys(hash, null, false, NO_KEY_RESTRICTIONS)) {
      return newArrayList(keys);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  List<Commit> commits(VersionStore versionStore, Hash hash) throws ReferenceNotFoundException {
    try (PaginationIterator<Commit> commits = versionStore.getCommits(hash, true)) {
      List<Commit> r = new ArrayList<>();
      while (commits.hasNext()) {
        Commit c = commits.next();
        r.add( // Persist's VersionStoreImpl does always add the commit ID to CommitMeta,
            // the current one does not, but that's not critical (and not incorrect), so
            // just tweak this check.
            Commit.builder()
                .from(c)
                .commitMeta(c.getCommitMeta().toBuilder().hash(c.getHash().asString()).build())
                .build());
      }
      return r;
    }
  }

  List<ReferenceInfo<CommitMeta>> namedRefs(VersionStore versionStore) {
    try (PaginationIterator<ReferenceInfo<CommitMeta>> refs =
        versionStore.getNamedRefs(GetNamedRefsParams.DEFAULT, null)) {
      return newArrayList(refs);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
