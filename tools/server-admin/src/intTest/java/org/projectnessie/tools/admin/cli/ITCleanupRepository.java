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

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.persist.Persist;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith({NessieServerAdminTestExtension.class, SoftAssertionsExtension.class})
class ITCleanupRepository extends AbstractContentTests<Object> {

  @InjectSoftAssertions private SoftAssertions soft;

  ITCleanupRepository(Persist persist) {
    super(persist, Object.class);
  }

  @Test
  public void testCleanup(QuarkusMainLauncher launcher) {
    var launchResult = launcher.launch("cleanup-repository");
    soft.assertThat(launchResult.exitCode()).isEqualTo(0);
    soft.assertThat(launchResult.getOutputStream())
        .contains(
            "Identifying referenced objects, processing unlimited commits per second, processing unlimited objects per second, expecting max 1000000 objects, estimated context heap pressure: 18.396 M")
        .anyMatch(
            s ->
                s.matches(
                    "Finished identifying referenced objects after PT.*. Processed 3 references, 3 commits, 2 objects, 0 contents."))
        .anyMatch(
            s ->
                s.matches(
                    "Purging unreferenced objects, referenced before .*, scanning unlimited objects per second, deleting unlimited objects per second, estimated context heap pressure: 5.713 M"))
        .anyMatch(
            s ->
                s.matches(
                    "Finished purging unreferenced objects after PT.*. Scanned 5 objects, 0 were deleted."));
  }

  @Test
  public void testCleanupParams(QuarkusMainLauncher launcher) {
    var launchResult =
        launcher.launch(
            "cleanup-repository",
            CleanupRepository.DRY_RUN,
            CleanupRepository.COMMIT_RATE,
            "11",
            CleanupRepository.OBJ_RATE,
            "12",
            CleanupRepository.SCAN_OBJ_RATE,
            "13",
            CleanupRepository.PURGE_OBJ_RATE,
            "14",
            CleanupRepository.REFERENCED_GRACE,
            "P10D",
            CleanupRepository.OBJ_COUNT,
            "1000");
    soft.assertThat(launchResult.exitCode()).isEqualTo(0);
    soft.assertThat(launchResult.getOutputStream())
        .contains(
            "Identifying referenced objects, processing 11 commits per second, processing 12 objects per second, expecting max 1000 objects, estimated context heap pressure: 12.688 M")
        .anyMatch(
            s ->
                s.matches(
                    "Finished identifying referenced objects after PT.*. Processed 3 references, 3 commits, 2 objects, 0 contents."))
        .anyMatch(
            s ->
                s.matches(
                    "Dry-run cleanup unreferenced objects, referenced before .*, scanning 13 objects per second, deleting 14 objects per second, estimated context heap pressure: 0.006 M"))
        .anyMatch(
            s ->
                s.matches(
                    "Finished purging unreferenced objects after PT.*. Scanned 5 objects, 0 were deleted."));
  }
}
