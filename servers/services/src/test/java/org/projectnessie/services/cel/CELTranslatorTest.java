/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.services.cel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;

public class CELTranslatorTest {

  @Test
  public void nullChecks() {
    assertThatThrownBy(() -> CELTranslator.from((CommitLogParams) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Commit log filtering params must be non-null");

    assertThatThrownBy(() -> CELTranslator.from((EntriesParams) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Entries filtering params must be non-null");
  }

  @Test
  public void emptyCommitLogParams() {
    assertThat(CELTranslator.from(CommitLogParams.empty())).isEqualTo("");
  }

  @Test
  public void emptyEntriesParams() {
    assertThat(CELTranslator.from(EntriesParams.empty())).isEqualTo("");
  }

  @Test
  public void commitLogParamsExprBuilding() {

    assertThat(CELTranslator.from(CommitLogParams.builder().authors(ImmutableList.of("")).build()))
        .isEqualTo("commit.author==''");

    assertThat(
            CELTranslator.from(
                CommitLogParams.builder().authors(ImmutableList.of("author1")).build()))
        .isEqualTo("commit.author=='author1'");

    assertThat(
            CELTranslator.from(
                CommitLogParams.builder()
                    .authors(ImmutableList.of("author1", "author2", "author3"))
                    .build()))
        .isEqualTo(
            "(commit.author=='author1' || commit.author=='author2' || commit.author=='author3')");

    assertThat(
            CELTranslator.from(
                CommitLogParams.builder().committers(ImmutableList.of("c1")).build()))
        .isEqualTo("commit.committer=='c1'");

    assertThat(
            CELTranslator.from(
                CommitLogParams.builder().committers(ImmutableList.of("c1", "c2", "c3")).build()))
        .isEqualTo("(commit.committer=='c1' || commit.committer=='c2' || commit.committer=='c3')");

    assertThat(
            CELTranslator.from(
                CommitLogParams.builder()
                    .authors(ImmutableList.of("a1", "a2"))
                    .committers(ImmutableList.of("c1", "c2"))
                    .build()))
        .isEqualTo(
            "(commit.author=='a1' || commit.author=='a2') && (commit.committer=='c1' || commit.committer=='c2')");

    Instant now = Instant.now();
    Instant fiveMinLater = now.plus(5, ChronoUnit.MINUTES);

    assertThat(CELTranslator.from(CommitLogParams.builder().after(now).build()))
        .isEqualTo(String.format("timestamp(commit.commitTime) > timestamp('%s')", now));

    assertThat(CELTranslator.from(CommitLogParams.builder().before(fiveMinLater).build()))
        .isEqualTo(String.format("timestamp(commit.commitTime) < timestamp('%s')", fiveMinLater));

    assertThat(
            CELTranslator.from(CommitLogParams.builder().after(now).before(fiveMinLater).build()))
        .isEqualTo(
            String.format(
                "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                now, fiveMinLater));

    assertThat(
            CELTranslator.from(
                CommitLogParams.builder()
                    .authors(ImmutableList.of("a1", "a2"))
                    .committers(ImmutableList.of("c1", "c2"))
                    .after(now)
                    .before(fiveMinLater)
                    .build()))
        .isEqualTo(
            String.format(
                "(commit.author=='a1' || commit.author=='a2') && (commit.committer=='c1' || commit.committer=='c2') "
                    + "&& timestamp(commit.commitTime) > timestamp('%s') "
                    + "&& timestamp(commit.commitTime) < timestamp('%s')",
                now, fiveMinLater));
  }
}
