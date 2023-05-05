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
package org.projectnessie.quarkus.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith(NessieCliTestExtension.class)
class ITNessieInfo {

  @Test
  @Launch("info")
  public void testNoInMemoryWarning(LaunchResult result) {
    // This test class uses MONGO
    assertThat(result.getErrorOutput()).doesNotContain("INMEMORY");
  }

  @Test
  @Launch("info")
  public void testNoAncestorHash(LaunchResult result, DatabaseAdapter adapter) {
    assertThat(result.getOutput()).contains(adapter.noAncestorHash().asString());
  }

  @Test
  public void testMainHash(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Hash hash =
        adapter
            .commit(
                ImmutableCommitParams.builder()
                    .toBranch(BranchName.of("main"))
                    .commitMetaSerialized(ByteString.copyFrom(new byte[] {1, 2, 3}))
                    .build())
            .getCommitHash();

    LaunchResult result = launcher.launch("info");
    assertThat(result.getOutput()).contains(hash.asString());
  }
}
