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
package org.projectnessie.events.service.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.events.api.ImmutableReference;
import org.projectnessie.events.api.Reference;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.NamedRef;

class TestReferenceMapping {

  @ParameterizedTest
  @MethodSource
  void map(NamedRef namedRef, Reference expected) {
    Reference actual = ReferenceMapping.map(namedRef);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> map() {
    return Stream.of(
        Arguments.of(
            org.projectnessie.versioned.BranchName.of("main"),
            ImmutableReference.builder()
                .type(Reference.BRANCH)
                .fullName("refs/heads/main")
                .simpleName("main")
                .build()),
        Arguments.of(
            org.projectnessie.versioned.TagName.of("v1.0.0"),
            ImmutableReference.builder()
                .type(Reference.TAG)
                .fullName("refs/tags/v1.0.0")
                .simpleName("v1.0.0")
                .build()),
        Arguments.of(
            DetachedRef.INSTANCE,
            ImmutableReference.builder().type("DETACHED").simpleName("DETACHED").build()),
        Arguments.of(
            (NamedRef) () -> "weird",
            ImmutableReference.builder().type("UNKNOWN").simpleName("weird").build()));
  }
}
