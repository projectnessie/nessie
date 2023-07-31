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
package org.projectnessie.versioned;

import static java.time.Instant.EPOCH;
import static java.time.Instant.ofEpochSecond;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.N_TH_PARENT;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.N_TH_PREDECESSOR;
import static org.projectnessie.versioned.RelativeCommitSpec.Type.TIMESTAMP_MILLIS_EPOCH;
import static org.projectnessie.versioned.RelativeCommitSpec.parseRelativeSpecs;
import static org.projectnessie.versioned.RelativeCommitSpec.relativeCommitSpec;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestRelativeCommitSpec {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void commitSpec() {
    soft.assertThat(relativeCommitSpec('~', "42"))
        .isEqualTo(relativeCommitSpec(N_TH_PREDECESSOR, 42L, EPOCH));
    soft.assertThat(relativeCommitSpec('^', "2"))
        .isEqualTo(relativeCommitSpec(N_TH_PARENT, 2L, EPOCH));
    soft.assertThat(relativeCommitSpec('*', "42123"))
        .isEqualTo(relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0L, ofEpochSecond(42, 123000000L)));
    soft.assertThat(relativeCommitSpec('*', "1970-01-01T00:00:42.123456789Z"))
        .isEqualTo(relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0L, ofEpochSecond(42, 123456789L)));

    soft.assertThatIllegalArgumentException().isThrownBy(() -> relativeCommitSpec('~', "0"));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> relativeCommitSpec('^', "0"));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> relativeCommitSpec('~', "-42"));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> relativeCommitSpec('^', "-42"));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> relativeCommitSpec('*', "boom"));
  }

  @ParameterizedTest
  @MethodSource("fromString")
  public void fromString(String relativeSpecs, List<RelativeCommitSpec> expected) {
    soft.assertThat(parseRelativeSpecs(relativeSpecs)).isEqualTo(expected);
  }

  static Stream<Arguments> fromString() {
    return Stream.of(
        arguments("", emptyList()),
        arguments("~1", singletonList(relativeCommitSpec(N_TH_PREDECESSOR, 1, Instant.EPOCH))),
        arguments("^2", singletonList(relativeCommitSpec(N_TH_PARENT, 2, Instant.EPOCH))),
        arguments(
            "*42123",
            singletonList(
                relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0, ofEpochSecond(42, 123000000L)))),
        arguments(
            "*1970-01-01T00:00:42.123Z",
            singletonList(
                relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0, ofEpochSecond(42, 123000000)))),
        arguments(
            "*1970-01-01T00:00:42.123456Z",
            singletonList(
                relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0, ofEpochSecond(42, 123456000)))),
        arguments(
            "*1970-01-01T00:00:42.123456789Z",
            singletonList(
                relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0, ofEpochSecond(42, 123456789)))),
        arguments(
            "*1970-01-01T00:00:42.1234567Z~23^2",
            asList(
                relativeCommitSpec(TIMESTAMP_MILLIS_EPOCH, 0, ofEpochSecond(42, 123456700)),
                relativeCommitSpec(N_TH_PREDECESSOR, 23, Instant.EPOCH),
                relativeCommitSpec(N_TH_PARENT, 2, Instant.EPOCH))));
  }
}
