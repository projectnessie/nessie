/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.ALL_FLAGS;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.CANDIDATE;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.COMMIT_A;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.COMMIT_B;
import static org.projectnessie.versioned.storage.common.logic.ShallowCommit.RESULT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestShallowCommit {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void setAllFlagsIfAnyMissing() {
    ShallowCommit commit = commit();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE | RESULT)).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE | RESULT);
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE)).isFalse();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(RESULT)).isFalse();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE | RESULT)).isFalse();

    commit = commit();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE)).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE);
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE)).isFalse();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(RESULT)).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE | RESULT);
    soft.assertThat(commit.setAllFlagsIfAnyMissing(RESULT)).isFalse();
  }

  @Test
  void clearFlagsIfAnySet() {
    ShallowCommit commit = commit();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE | RESULT)).isFalse();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE)).isFalse();
    soft.assertThat(commit.clearFlagsIfAnySet(RESULT)).isFalse();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE | RESULT)).isFalse();

    commit = commit();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE | RESULT)).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE | RESULT);
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE | RESULT)).isTrue();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE | RESULT)).isFalse();

    commit = commit();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE)).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE);
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE | RESULT)).isTrue();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE | RESULT)).isFalse();

    commit = commit();
    soft.assertThat(commit.setAllFlagsIfAnyMissing(CANDIDATE)).isTrue();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE)).isTrue();
    soft.assertThat(commit.clearFlagsIfAnySet(CANDIDATE)).isFalse();
  }

  @Test
  void setClearResult() {
    ShallowCommit commit = commit();
    soft.assertThat(commit.clearResult()).isFalse();
    soft.assertThat(commit.setResult()).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(RESULT);
    soft.assertThat(commit.setResult()).isFalse();
    soft.assertThat(commit.clearResult()).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(0);
    soft.assertThat(commit.clearResult()).isFalse();
  }

  @Test
  void setStale() {
    ShallowCommit commit = commit();
    soft.assertThat(commit.setCandidate()).isTrue();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE);
  }

  @Test
  void setClearFlags() {
    ShallowCommit commit = commit();

    soft.assertThat(commit.isAnyFlagSet(ALL_FLAGS)).isFalse();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE)).isFalse();
    soft.assertThat(commit.flags()).isEqualTo(0);

    commit.setCandidate();
    soft.assertThat(commit.flags()).isEqualTo(CANDIDATE);

    soft.assertThat(commit.isAnyFlagSet(ALL_FLAGS)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE)).isTrue();

    commit.clearCandidate();
    soft.assertThat(commit.flags()).isEqualTo(0);

    int multiple = CANDIDATE | COMMIT_A | COMMIT_B;
    commit.setCandidate();
    commit.setCommitA();
    commit.setCommitB();
    soft.assertThat(commit.flags()).isEqualTo(multiple);

    soft.assertThat(commit.isAnyFlagSet(multiple)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(CANDIDATE | COMMIT_A)).isTrue();

    commit.clearCandidate();
    soft.assertThat(commit.flags()).isEqualTo(COMMIT_A | COMMIT_B);

    soft.assertThat(commit.isAnyFlagSet(multiple)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(COMMIT_A)).isTrue();
    soft.assertThat(commit.isAnyFlagSet(COMMIT_A | COMMIT_B)).isTrue();
  }

  private ShallowCommit commit() {
    return new ShallowCommit(randomObjId(), new ObjId[] {randomObjId()}, 1L);
  }
}
