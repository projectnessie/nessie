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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.DEFAULT_PARENTS_PER_COMMIT;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith(NessieServerAdminTestExtension.class)
class ITNessieInfo {

  @Test
  @Launch("info")
  public void testNoInMemoryWarning(LaunchResult result) {
    // This test class uses MONGO
    assertThat(result.getErrorOutput()).doesNotContain("IN_MEMORY");
  }

  @Test
  @Launch("info")
  public void testNoAncestorHash(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Repository created:")
        .contains("Default branch head commit ID:     " + EMPTY_OBJ_ID)
        .contains("Default branch commit count:       0")
        .containsPattern(
            "Version-store type: {16}(ROCKSDB|DYNAMODB|MONGODB|CASSANDRA|JDBC|BIGTABLE)")
        .contains("Default branch:                    main")
        .contains("Parent commit IDs per commit:      " + DEFAULT_PARENTS_PER_COMMIT);
  }

  @Test
  public void testMainHash(QuarkusMainLauncher launcher, Persist persist)
      throws CommitConflictException,
          ObjNotFoundException,
          RefNotFoundException,
          RefConditionFailedException {
    CommitObj head =
        commitLogic(persist)
            .doCommit(
                newCommitBuilder()
                    .parentCommitId(EMPTY_OBJ_ID)
                    .message("hello")
                    .headers(EMPTY_COMMIT_HEADERS)
                    .build(),
                emptyList());
    Reference reference = persist.fetchReference("refs/heads/main");
    referenceLogic(persist).assignReference(requireNonNull(reference), requireNonNull(head).id());

    LaunchResult result = launcher.launch("info");
    assertThat(result.getOutput())
        .contains("Repository created:")
        .contains("Default branch head commit ID:     " + head.id())
        .contains("Default branch commit count:       1")
        .containsPattern(
            "Version-store type: {16}(ROCKSDB|DYNAMODB|MONGODB|CASSANDRA|JDBC|BIGTABLE)")
        .contains("Default branch:                    main")
        .contains("Parent commit IDs per commit:      " + DEFAULT_PARENTS_PER_COMMIT);
  }
}
