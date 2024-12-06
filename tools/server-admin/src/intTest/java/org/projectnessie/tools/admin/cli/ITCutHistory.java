/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith({NessieServerAdminTestExtension.class, SoftAssertionsExtension.class})
class ITCutHistory extends AbstractContentTests<CheckContentEntry> {

  @InjectSoftAssertions private SoftAssertions soft;

  ITCutHistory(Persist persist) {
    super(persist, CheckContentEntry.class);
  }

  @Test
  public void testEmptyRepo(QuarkusMainLauncher launcher) {
    var launchResult = launcher.launch("cut-history", "--depth", "1");
    soft.assertThat(launchResult.exitCode()).isEqualTo(0);
    soft.assertThat(launchResult.getOutputStream()).contains("Updated 0 commits.");
  }

  @Test
  public void testShallowRepo(QuarkusMainLauncher launcher) throws Exception {
    commit(IcebergTable.of("111", 1, 2, 3, 4, UUID.randomUUID().toString()));
    commit(IcebergTable.of("222", 1, 2, 3, 4, UUID.randomUUID().toString()));
    var launchResult = launcher.launch("cut-history", "--ref", "main", "--depth", "10");
    soft.assertThat(launchResult.exitCode()).isEqualTo(0);
    soft.assertThat(launchResult.getOutputStream()).contains("Updated 0 commits.");
  }

  @Test
  public void testCutMain(QuarkusMainLauncher launcher) throws Exception {
    Set<Object> locations = new HashSet<>();
    List<Hash> commitHashes = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String location = "loc-" + i;
      locations.add(location);
      CommitObj commit =
          commit(IcebergTable.of(location, i, 1, 1, 1, UUID.randomUUID().toString()));
      commitHashes.addFirst(objIdToHash(commit.id()));
    }

    var launchResult = launcher.launch("cut-history", "--depth", "5");
    soft.assertThat(launchResult.exitCode()).isEqualTo(0);
    // 20 commits need rewriting because of the default 20 entries in the commit "tail".
    soft.assertThat(launchResult.getOutputStream()).contains("Updated 20 commits.");

    VersionStoreImpl store = new VersionStoreImpl(persist());

    // 5 preserved commits plus 20 in the commit tail
    int histDepth = 25;
    for (int i = 0; i < histDepth; i++) {
      Hash start = commitHashes.get(i);
      List<Hash> log = new ArrayList<>();
      store.getCommits(start, false).forEachRemaining(c -> log.add(c.getHash()));
      soft.assertThat(log).containsExactlyElementsOf(commitHashes.subList(i, histDepth));
    }

    Set<String> collectedLocations = new HashSet<>();
    store
        .getKeys(getMainHead(), null, true, VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS)
        .forEachRemaining(
            e -> {
              if (e.getContent() instanceof IcebergTable) {
                collectedLocations.add(((IcebergTable) e.getContent()).getMetadataLocation());
              }
            });
    soft.assertThat(collectedLocations).isEqualTo(locations);
  }

  @Test
  public void testCutAcrossMerge(QuarkusMainLauncher launcher) throws Exception {
    CommitLogic commitLogic = commitLogic(persist());
    commit(IcebergTable.of("111", 1, 2, 3, 4, UUID.randomUUID().toString()));
    CommitObj commit1 = requireNonNull(commitLogic.fetchCommit(hashToObjId(getMainHead())));
    commit(IcebergTable.of("222", 1, 2, 3, 4, UUID.randomUUID().toString()));
    commit(IcebergTable.of("333", 1, 2, 3, 4, UUID.randomUUID().toString()));
    CommitObj commit3 = requireNonNull(commitLogic.fetchCommit(hashToObjId(getMainHead())));
    commit(IcebergTable.of("444", 1, 2, 3, 4, UUID.randomUUID().toString()));

    persist()
        .upsertObj(
            CommitObj.commitBuilder().from(commit3).addSecondaryParents(commit1.id()).build());

    var launchResult = launcher.launch("cut-history", "--depth", "1");
    soft.assertThat(launchResult.exitCode()).isEqualTo(1);
    soft.assertThat(launchResult.getErrorOutput())
        .contains(String.format("Unable to reset parents in merge commit '%s'", commit3.id()));
  }

  @Test
  public void testCutAcrossRoot(QuarkusMainLauncher launcher) throws Exception {
    commit(IcebergTable.of("111", 1, 2, 3, 4, UUID.randomUUID().toString()));
    commit(IcebergTable.of("222", 1, 2, 3, 4, UUID.randomUUID().toString()));
    var launchResult = launcher.launch("cut-history", "--ref", "main", "--depth", "1");
    soft.assertThat(launchResult.exitCode()).isEqualTo(0);
    soft.assertThat(launchResult.getErrorOutput()).contains("Encountered root commit");
  }
}
