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
package org.projectnessie.services.impl;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.CommitMeta.fromMessage;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;

@ExtendWith(SoftAssertionsExtension.class)
class TestCommitMetaUpdater {
  @InjectSoftAssertions protected SoftAssertions soft;

  static Instant NOW = now();
  static Instant AUTHOR_TIME = now().minus(30, DAYS);
  static Instant AUTHOR_TIME_OLDER = now().minus(31, DAYS);

  @ParameterizedTest
  @MethodSource("rewriteSingle")
  void rewriteSingle(String committer, CommitMeta inNessie, CommitMeta expected) {
    CommitMeta parameter = null;
    CommitMetaUpdater updater = new CommitMetaUpdater(committer, NOW, parameter, p -> null);
    soft.assertThat(updater.rewriteSingle(inNessie)).isEqualTo(expected);
    soft.assertThat(updater.squash(singletonList(inNessie))).isEqualTo(expected);

    parameter = fromMessage("forced message");
    updater = new CommitMetaUpdater(committer, NOW, parameter, p -> null);
    CommitMeta expectedUpdated =
        CommitMeta.builder().from(expected).message("forced message").build();
    soft.assertThat(updater.rewriteSingle(inNessie)).isEqualTo(expectedUpdated);
    soft.assertThat(updater.squash(singletonList(inNessie))).isEqualTo(expectedUpdated);

    parameter = CommitMeta.builder().message("").authorTime(AUTHOR_TIME_OLDER).build();
    updater = new CommitMetaUpdater(committer, NOW, parameter, p -> null);
    expectedUpdated = CommitMeta.builder().from(expected).authorTime(AUTHOR_TIME_OLDER).build();
    soft.assertThat(updater.rewriteSingle(inNessie)).isEqualTo(expectedUpdated);
    soft.assertThat(updater.squash(singletonList(inNessie))).isEqualTo(expectedUpdated);

    parameter = CommitMeta.builder().message("").addAllAuthors("myself").build();
    updater = new CommitMetaUpdater(committer, NOW, parameter, p -> null);
    expectedUpdated =
        CommitMeta.builder().from(expected).allAuthors(singletonList("myself")).build();
    soft.assertThat(updater.rewriteSingle(inNessie)).isEqualTo(expectedUpdated);
    soft.assertThat(updater.squash(singletonList(inNessie))).isEqualTo(expectedUpdated);
  }

  @ParameterizedTest
  @MethodSource("squash")
  void squash(String committer, List<CommitMeta> inNessie, CommitMeta expected) {
    CommitMeta parameter = null;
    CommitMetaUpdater updater =
        new CommitMetaUpdater(committer, NOW, parameter, p -> "Default message for " + p);
    soft.assertThat(updater.squash(inNessie)).isEqualTo(expected);

    parameter = fromMessage("forced message");
    updater = new CommitMetaUpdater(committer, NOW, parameter, p -> "Default message for " + p);
    CommitMeta expectedUpdated =
        CommitMeta.builder().from(expected).message("forced message").build();
    soft.assertThat(updater.squash(inNessie)).isEqualTo(expectedUpdated);

    parameter = CommitMeta.builder().message("").authorTime(AUTHOR_TIME_OLDER).build();
    updater = new CommitMetaUpdater(committer, NOW, parameter, p -> "Default message for " + p);
    expectedUpdated = CommitMeta.builder().from(expected).authorTime(AUTHOR_TIME_OLDER).build();
    soft.assertThat(updater.squash(inNessie)).isEqualTo(expectedUpdated);

    parameter = CommitMeta.builder().message("").addAllAuthors("myself").build();
    updater = new CommitMetaUpdater(committer, NOW, parameter, p -> "Default message for " + p);
    expectedUpdated =
        CommitMeta.builder().from(expected).allAuthors(singletonList("myself")).build();
    soft.assertThat(updater.squash(inNessie)).isEqualTo(expectedUpdated);
  }

  static Stream<Arguments> rewriteSingle() {
    return Stream.of(
        // 1
        arguments(
            "c1",
            fromMessage("just a message"),
            CommitMeta.builder()
                .commitTime(NOW)
                .committer("c1")
                .authorTime(NOW)
                .addAllAuthors("c1")
                .message("just a message")
                .build()),
        // 2
        arguments(
            "c2",
            CommitMeta.builder().message("just a message").addAllAuthors("author1").build(),
            CommitMeta.builder()
                .commitTime(NOW)
                .committer("c2")
                .authorTime(NOW)
                .addAllAuthors("author1")
                .message("just a message")
                .build()),
        // 3
        arguments(
            "c3",
            CommitMeta.builder().message("just a message").authorTime(AUTHOR_TIME).build(),
            CommitMeta.builder()
                .commitTime(NOW)
                .committer("c3")
                .authorTime(AUTHOR_TIME)
                .addAllAuthors("c3")
                .message("just a message")
                .build()));
  }

  static Stream<Arguments> squash() {
    return Stream.of(
        // 1
        arguments(
            "c1",
            asList(fromMessage("foo"), fromMessage("bar")),
            CommitMeta.builder()
                .commitTime(NOW)
                .committer("c1")
                .authorTime(NOW)
                .addAllAuthors("c1")
                .message("Default message for 2")
                .build()),
        // 2
        arguments(
            "c2",
            asList(
                CommitMeta.builder().message("foo").addAllAuthors("author1").build(),
                CommitMeta.builder().message("bar").authorTime(AUTHOR_TIME_OLDER).build(),
                CommitMeta.builder()
                    .message("foo foo")
                    .authorTime(AUTHOR_TIME)
                    .addAllAuthors("author1")
                    .build(),
                CommitMeta.builder()
                    .message("baz")
                    .authorTime(AUTHOR_TIME_OLDER)
                    .addAllAuthors("author2", "author3")
                    .build()),
            CommitMeta.builder()
                .commitTime(NOW)
                .committer("c2")
                .authorTime(AUTHOR_TIME)
                .addAllAuthors("author1", "author2", "author3")
                .message("Default message for 4")
                .build()));
  }
}
