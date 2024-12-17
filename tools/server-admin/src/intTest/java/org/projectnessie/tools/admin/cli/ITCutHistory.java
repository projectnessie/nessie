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

import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
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
  public void testInvalidCommitHash(QuarkusMainLauncher launcher) {
    var launchResult = launcher.launch("cut-history", "--commit", "1122334455667788");
    soft.assertThat(launchResult.exitCode()).isEqualTo(1);
    soft.assertThat(launchResult.getOutputStream()).noneMatch(s -> s.matches(".*Rewrote.*"));
    soft.assertThat(launchResult.getErrorStream())
        .contains(
            "Unable to rewrite parents for 1122334455667788: "
                + "org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException: "
                + "Object with ID 1122334455667788 not found.");
  }

  @Test
  public void testCutMainAndCleanup(QuarkusMainLauncher launcher) throws Exception {
    Set<Object> locations = new HashSet<>();
    List<Hash> commitHashes = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String location = "loc-" + i;
      locations.add(location);
      CommitObj commit =
          commit(IcebergTable.of(location, i, 1, 1, 1, UUID.randomUUID().toString()));
      commitHashes.add(objIdToHash(commit.id()));
    }

    Hash cutPoint = commitHashes.get(50);
    var cutResult = launcher.launch("cut-history", "--commit", cutPoint.asString());
    soft.assertThat(cutResult.exitCode()).isEqualTo(0);
    // 19 commits need rewriting because of the default 20 entries in the commit "tail".
    // The commit whole last tail element is the cut point, does not need to be rewritten.
    soft.assertThat(cutResult.getOutputStream()).contains("Identified 19 related commits.");
    // 19 tail rewrites plus the cut point
    soft.assertThat(cutResult.getOutputStream()).contains("Rewrote 20 commits.");
    soft.assertThat(cutResult.getOutputStream())
        .contains("Removed parents from commit " + cutPoint.asString() + ".");
    soft.assertThat(cutResult.getOutputStream()).anyMatch(s -> s.matches("Completed in PT.*S."));

    var cleanResult = launcher.launch("cleanup-repository");
    soft.assertThat(cleanResult.exitCode()).isEqualTo(0);
    soft.assertThat(cleanResult.getOutputStream())
        .anyMatch(s -> s.matches(".*Scanned .* objects, 50 were deleted.*"));

    VersionStoreImpl store = new VersionStoreImpl(persist());

    for (int i = 50; i < commitHashes.size(); i++) {
      Hash start = commitHashes.get(i);
      List<Hash> log = new ArrayList<>();
      store.getCommits(start, false).forEachRemaining(c -> log.add(c.getHash()));
      soft.assertThat(log).containsExactlyElementsOf(commitHashes.subList(50, i + 1).reversed());
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
  public void testDryRun(QuarkusMainLauncher launcher) throws Exception {
    CommitObj c1 = commit(IcebergTable.of("loc1", 1, 1, 1, 1, UUID.randomUUID().toString()));
    CommitObj c2 = commit(IcebergTable.of("loc2", 1, 1, 1, 1, UUID.randomUUID().toString()));
    CommitObj c3 = commit(IcebergTable.of("loc3", 1, 1, 1, 1, UUID.randomUUID().toString()));

    var cutResult = launcher.launch("cut-history", "--dry-run", "--commit", c2.id().toString());
    soft.assertThat(cutResult.exitCode()).isEqualTo(0);
    soft.assertThat(cutResult.getOutputStream())
        .noneMatch(s -> s.matches(".*Rewrote .* commits.*"));
    soft.assertThat(cutResult.getOutputStream()).noneMatch(s -> s.matches(".*Removed parents.*"));

    CommitLogic commitLogic = commitLogic(persist());
    soft.assertThat(commitLogic.fetchCommit(c1.id())).isEqualTo(c1);
    soft.assertThat(commitLogic.fetchCommit(c2.id())).isEqualTo(c2);
    soft.assertThat(commitLogic.fetchCommit(c3.id())).isEqualTo(c3);
  }
}
