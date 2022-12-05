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
package org.projectnessie.versioned.persist.adapter;

import static java.lang.String.format;
import static java.util.Collections.shuffle;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter.NO_ANCESTOR;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.ReferencesUtil.IdentifyHeadsAndForkPoints;

@ExtendWith(SoftAssertionsExtension.class)
public class TestReferencesUtil {
  @InjectSoftAssertions protected SoftAssertions soft;

  static Hash commitId(int i) {
    return Hash.of(format("000000000000000000000000%08x", i));
  }

  static List<Arguments> scenarios() {
    List<Arguments> r = new ArrayList<>();

    Set<Hash> heads;
    Set<Hash> forks;
    Map<Hash, Hash> commits;

    // 1
    heads = new HashSet<>();
    forks = new HashSet<>();
    commits = new LinkedHashMap<>();
    r.add(arguments(HeadsAndForkPoints.of(heads, forks, 0L), commits));

    // 2
    heads = new HashSet<>();
    forks = new HashSet<>();
    commits = new LinkedHashMap<>();
    heads.add(commitId(1));
    commits.put(commitId(1), NO_ANCESTOR);
    r.add(arguments(HeadsAndForkPoints.of(heads, forks, 0L), commits));

    // 3
    heads = new HashSet<>();
    forks = new HashSet<>();
    commits = new LinkedHashMap<>();
    commits.put(commitId(1), NO_ANCESTOR);
    for (int i = 2; i < 16; i++) {
      commits.put(commitId(i), commitId(i - 1));
    }
    heads.add(commitId(15));
    r.add(arguments(HeadsAndForkPoints.of(heads, forks, 0L), commits));

    // 4 / independent branches
    heads = new HashSet<>();
    forks = new HashSet<>();
    commits = new LinkedHashMap<>();
    for (int branch = 0; branch < 5; branch++) {
      int commit = branch * 16;
      commits.put(commitId(commit), NO_ANCESTOR);
      for (int i = 2; i < 16; i++) {
        commit++;
        commits.put(commitId(commit), commitId(commit - 1));
      }
      heads.add(commitId(commit));
    }
    r.add(arguments(HeadsAndForkPoints.of(heads, forks, 0L), commits));

    // 5 / multiple branches from same ancestor / "main" on ancestor (--> not a detected head)
    heads = new HashSet<>();
    forks = new HashSet<>();
    commits = new LinkedHashMap<>();
    // "commits on main"
    commits.put(commitId(0), NO_ANCESTOR);
    int commit = 0;
    for (int i = 1; i < 16; i++) {
      commit++;
      commits.put(commitId(commit), commitId(commit - 1));
    }
    forks.add(commitId(commit));
    for (int branch = 1; branch <= 5; branch++) {
      int branchCommit = branch * 16;
      commits.put(commitId(branchCommit), commitId(commit));
      for (int i = 1; i < 16; i++) {
        branchCommit++;
        commits.put(commitId(branchCommit), commitId(branchCommit - 1));
      }
      heads.add(commitId(branchCommit));
    }
    r.add(arguments(HeadsAndForkPoints.of(heads, forks, 0L), commits));

    // 6 / multiple branches from same ancestor / "main" 1 commit ahead ancestor
    heads = new HashSet<>();
    forks = new HashSet<>();
    commits = new LinkedHashMap<>();
    // "commits on main"
    commits.put(commitId(0), NO_ANCESTOR);
    commit = 0;
    for (int i = 1; i < 16; i++) {
      commit++;
      commits.put(commitId(commit), commitId(commit - 1));
    }
    forks.add(commitId(commit));
    for (int branch = 1; branch <= 5; branch++) {
      int branchCommit = branch * 16;
      commits.put(commitId(branchCommit), commitId(commit));
      for (int i = 1; i < 16; i++) {
        branchCommit++;
        commits.put(commitId(branchCommit), commitId(branchCommit - 1));
      }
      heads.add(commitId(branchCommit));
    }
    commits.put(commitId(0x1000), commitId(commit));
    heads.add(commitId(0x1000));
    r.add(arguments(HeadsAndForkPoints.of(heads, forks, 0L), commits));

    return r;
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  public void identifyHeadsAndForkPointsFromScan(
      HeadsAndForkPoints expected, Map<Hash, Hash> commits) {
    IdentifyHeadsAndForkPoints identify = new IdentifyHeadsAndForkPoints(1000, 0L);
    commits.forEach(identify::handleCommit);
    HeadsAndForkPoints headsAndForkPoints = identify.finish();
    soft.assertThat(headsAndForkPoints).isEqualTo(expected);

    ArrayList<Entry<Hash, Hash>> commitsList = new ArrayList<>(commits.entrySet());

    // reverse commit order
    identify = new IdentifyHeadsAndForkPoints(1000, 0L);
    for (int i = commitsList.size() - 1; i >= 0; i--) {
      Map.Entry<Hash, Hash> commit = commitsList.get(i);
      identify.handleCommit(commit.getKey(), commit.getValue());
    }
    headsAndForkPoints = identify.finish();
    soft.assertThat(headsAndForkPoints).isEqualTo(expected);

    // Iterate with a couple of random processing orders. If this is flaky, it is definitely a bug!
    for (int i = 0; i < 12; i++) {
      shuffle(commitsList);
      identify = new IdentifyHeadsAndForkPoints(1000, 0L);
      for (Map.Entry<Hash, Hash> commit : commitsList) {
        identify.handleCommit(commit.getKey(), commit.getValue());
      }
      headsAndForkPoints = identify.finish();
      soft.assertThat(headsAndForkPoints).isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  public void identifyHeadsAndForkPointsFromCommitLogWalking(
      HeadsAndForkPoints expected, Map<Hash, Hash> commits) {
    IdentifyHeadsAndForkPoints identify = new IdentifyHeadsAndForkPoints(1000, 0L);
    Set<Hash> seenCommits = new HashSet<>();
    for (Hash head : expected.getHeads()) {
      for (Hash commit = head; !NO_ANCESTOR.equals(commit); ) {
        Hash parent = commits.get(commit);
        if (!identify.handleCommit(commit, parent)) {
          break;
        }
        soft.assertThat(seenCommits.add(commit)).isTrue();
        commit = parent;
      }
    }
    soft.assertThat(seenCommits).containsExactlyInAnyOrderElementsOf(commits.keySet());
    HeadsAndForkPoints headsAndForkPoints = identify.finish();
    soft.assertThat(headsAndForkPoints).isEqualTo(expected);
  }
}
