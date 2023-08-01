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
package org.projectnessie.services.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.N_TH_PREDECESSOR;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.TIMESTAMP_MILLIS_EPOCH;
import static org.projectnessie.versioned.RelativeCommitSpec.relativeCommitSpec;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.Hash;

class TestParsedHash {

  private static final Hash NO_ANCESTOR = Hash.of("12345678");

  public static Stream<Arguments> parseGoodCases() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of("", null),
        Arguments.of("cafeBABE", ParsedHash.of(Hash.of("cafebabe"))),
        Arguments.of("~1", ParsedHash.of(relativeCommitSpec(N_TH_PREDECESSOR, "1"))),
        Arguments.of(
            "cafeBABE*2023-07-29T12:34:56.789Z~1",
            ParsedHash.of(
                Hash.of("cafebabe"),
                relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, "2023-07-29T12:34:56.789Z"),
                relativeCommitSpec(N_TH_PREDECESSOR, "1"))));
  }

  @ParameterizedTest
  @MethodSource
  void parseGoodCases(String hashOrRelativeSpec, ParsedHash expected) {
    Optional<ParsedHash> parse = ParsedHash.parse(hashOrRelativeSpec, NO_ANCESTOR);
    assertThat(parse.orElse(null)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {" ", "123", "cafeBABE?1"})
  void parseBadCases(String hashOrRelativeSpec) {
    assertThatThrownBy(() -> ParsedHash.parse(hashOrRelativeSpec, NO_ANCESTOR))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void parseNoAncestor() {
    Optional<ParsedHash> parsed = ParsedHash.parse(NO_ANCESTOR.asString() + "~1", NO_ANCESTOR);
    assertThat(parsed.flatMap(ParsedHash::getAbsolutePart)).containsSame(NO_ANCESTOR);
  }
}
