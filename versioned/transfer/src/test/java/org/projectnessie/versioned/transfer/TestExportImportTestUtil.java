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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.Hash;

class TestExportImportTestUtil {
  @ParameterizedTest
  @CsvSource(value = {"0,00000000", "1,00000001", "65536,00010000", "-1,ffffffff"})
  void intToHash(int i, String hash) {
    assertThat(ExportImportTestUtil.intToHash(i)).extracting(Hash::asString).isEqualTo(hash);
  }

  static Stream<Arguments> parentCommitHash() {
    return Stream.of(
        arguments(0, 0, "ffffffff"),
        arguments(0, 5, "ffffffff"),
        arguments(1, 0, "00000000"),
        arguments(1, 5, "00000000"),
        arguments(3, 0, "00000002"),
        arguments(3, 5, "00000002"),
        arguments(65535, 0, "0000fffe"),
        arguments(65535, 5, "0000fffe"),
        arguments(0x10000, 0, "0000ffff"),
        arguments(0x10000, 1, "00000001"),
        arguments(0x10000, 5, "00000005"),
        arguments(0x20000, 0, "0001ffff"),
        arguments(0x20000, 1, "00000002"),
        arguments(0x20000, 5, "0000000a"),
        arguments(0x50000, 0, "0004ffff"),
        arguments(0x50000, 1, "00000005"),
        arguments(0x50000, 5, "00000019"),
        arguments(0x20001, 0, "00020000"),
        arguments(0x20001, 1, "00020000"),
        arguments(0x20001, 5, "00020000"),
        arguments(0x20010, 0, "0002000f"),
        arguments(0x20010, 1, "0002000f"),
        arguments(0x20010, 5, "0002000f"),
        arguments(0x2ffff, 0, "0002fffe"),
        arguments(0x2ffff, 1, "0002fffe"),
        arguments(0x2ffff, 5, "0002fffe"));
  }

  @ParameterizedTest
  @MethodSource("parentCommitHash")
  void parentCommitHash(int branchAndCommit, int commitsBetweenBranches, String hash) {
    assertThat(ExportImportTestUtil.parentCommitHash(branchAndCommit, commitsBetweenBranches))
        .extracting(Hash::asString)
        .isEqualTo(hash);
  }

  static Stream<Arguments> expectedParents() {
    return Stream.of(
        arguments(0, 0, singletonList("ffffffff")),
        arguments(0, 5, singletonList("ffffffff")),
        arguments(1, 0, asList("00000000", "ffffffff")),
        arguments(1, 5, asList("00000000", "ffffffff")),
        arguments(2, 0, asList("00000001", "00000000", "ffffffff")),
        arguments(2, 5, asList("00000001", "00000000", "ffffffff")),
        arguments(3, 0, asList("00000002", "00000001", "00000000")),
        arguments(3, 5, asList("00000002", "00000001", "00000000")),
        arguments(65535, 0, asList("0000fffe", "0000fffd", "0000fffc")),
        arguments(65535, 5, asList("0000fffe", "0000fffd", "0000fffc")),
        arguments(0x10000, 1, asList("00000001", "00000000", "ffffffff")),
        arguments(0x10000, 5, asList("00000005", "00000004", "00000003")),
        arguments(0x20000, 1, asList("00000002", "00000001", "00000000")),
        arguments(0x20000, 5, asList("0000000a", "00000009", "00000008")),
        arguments(0x50000, 1, asList("00000005", "00000004", "00000003")),
        arguments(0x50000, 5, asList("00000019", "00000018", "00000017")),
        arguments(0x20001, 1, asList("00020000", "00000002", "00000001")),
        arguments(0x20001, 5, asList("00020000", "0000000a", "00000009")),
        arguments(0x20010, 1, asList("0002000f", "0002000e", "0002000d")),
        arguments(0x20010, 5, asList("0002000f", "0002000e", "0002000d")),
        arguments(0x2ffff, 1, asList("0002fffe", "0002fffd", "0002fffc")),
        arguments(0x2ffff, 5, asList("0002fffe", "0002fffd", "0002fffc")));
  }

  @ParameterizedTest
  @MethodSource("expectedParents")
  void expectedParents(int i, int commitsBetweenBranches, List<String> expected) {
    assertThat(ExportImportTestUtil.expectedParents(i, commitsBetweenBranches, 3))
        .map(Hash::asString)
        .isEqualTo(expected);
  }

  static Stream<Arguments> commitSeq() {
    return Stream.of(
        arguments(0, 0, 0L),
        arguments(0xffff, 0, 0xffffL),
        arguments(0x2ffff, 0, 0x2ffffL),
        arguments(0x20000, 0, 0x20000L),
        arguments(0x20000, 3, 0x7L),
        arguments(0x2ffff, 3, 0x7L + 0xffffL));
  }

  @ParameterizedTest
  @MethodSource("commitSeq")
  void commitSeq(int i, int commitsBetweenBranches, long expected) {
    assertThat(ExportImportTestUtil.commitSeq(i, commitsBetweenBranches)).isEqualTo(expected);
  }
}
