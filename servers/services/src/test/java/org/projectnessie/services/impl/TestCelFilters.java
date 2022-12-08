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

import static java.util.Collections.emptySet;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCelFilters {
  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<Arguments> filterOnContentKey() {
    return Stream.of(
        arguments("false", emptySet(), ImmutableSet.of(ContentKey.of("foo"), ContentKey.of("bar"))),
        arguments("true", ImmutableSet.of(ContentKey.of("foo"), ContentKey.of("bar")), emptySet()),
        arguments(
            "key.name=='table'",
            ImmutableSet.of(ContentKey.of("foo", "table"), ContentKey.of("bar", "table")),
            ImmutableSet.of(
                ContentKey.of("foo", "udf"),
                ContentKey.of("bar", "view"),
                ContentKey.of("foo"),
                ContentKey.of("bar"))),
        arguments(
            "key.namespace=='foo'",
            ImmutableSet.of(ContentKey.of("foo", "table"), ContentKey.of("foo", "foo")),
            ImmutableSet.of(
                ContentKey.of("foo", "bar", "udf"),
                ContentKey.of("bar", "view"),
                ContentKey.of("foo"),
                ContentKey.of("bar"))));
  }

  @ParameterizedTest
  @MethodSource
  void filterOnContentKey(String filter, Set<ContentKey> mustMatch, Set<ContentKey> mustNotMatch) {
    Predicate<ContentKey> predicate = BaseApiImpl.filterOnContentKey(filter);
    soft.assertThat(mustMatch).allMatch(predicate);
    soft.assertThat(mustNotMatch).noneMatch(predicate);
  }
}
