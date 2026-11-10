/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.gc.expire;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSharedBaseLocations {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> sharedBaseLocations() {
    return Stream.of(
        // distinct locations, sibling locations with a common prefix, multiple per content
        arguments(
            Map.of(
                "a", List.of("s3://bucket/db/t/"),
                "b", List.of("s3://bucket/db/t_1/", "s3://bucket/db/t-2/"),
                "c", List.of("s3://bucket/db/t.3"),
                "d", List.of("s3://other/db/t/")),
            Set.of()),
        // same location, used by multiple contents
        arguments(
            Map.of(
                "a", List.of("s3://bucket/db/t/", "s3://bucket/db/a/"),
                "b", List.of("s3://bucket/db/t/"),
                "c", List.of("s3://bucket/db/c/")),
            Set.of("s3://bucket/db/t/")),
        // nested locations of different contents
        arguments(
            Map.of(
                "a", List.of("s3://bucket/db/"),
                "b", List.of("s3://bucket/db/t/"),
                "c", List.of("s3://bucket/db/t/sub/dir/"),
                "d", List.of("s3://bucket/db2/")),
            Set.of("s3://bucket/db/", "s3://bucket/db/t/", "s3://bucket/db/t/sub/dir/")),
        // nested locations of the same content
        arguments(
            Map.of(
                "a", List.of("s3://bucket/db/t/", "s3://bucket/db/t/sub/"),
                "b", List.of("s3://bucket/db/u/")),
            Set.of("s3://bucket/db/t/", "s3://bucket/db/t/sub/")),
        // bucket root
        arguments(
            Map.of("a", List.of("s3://bucket"), "b", List.of("s3://bucket/db/t/")),
            Set.of("s3://bucket", "s3://bucket/db/t/")));
  }

  @ParameterizedTest
  @MethodSource
  public void sharedBaseLocations(Map<String, List<String>> contents, Set<String> expected) {
    SharedBaseLocations shared =
        new SharedBaseLocations(
            contents.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().stream().map(StorageUri::of).toList())));

    soft.assertThat(
            contents.values().stream()
                .flatMap(List::stream)
                .distinct()
                .filter(l -> shared.isShared(StorageUri.of(l))))
        .containsExactlyInAnyOrderElementsOf(expected);
    soft.assertThat(shared.size()).isEqualTo(expected.size());
    soft.assertThat(shared.isShared(StorageUri.of("s3://bucket/unknown/"))).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "s3://bucket/db/t/, s3a://bucket/db/t, true",
    "s3://bucket/db/t/, s3n://bucket/db/t/, true",
    "s3://bucket/db/t/, S3://bucket/db/t/, true",
    "abfs://c@account.dfs.core.windows.net/t/, abfss://c@account.dfs.core.windows.net/t/, true",
    "wasb://c@account.blob.core.windows.net/t/, wasbs://c@account.blob.core.windows.net/t/, true",
    "file:/tmp/wh/t/, file:///tmp/wh/t, true",
    "s3://bucket/db/t/, s3a://bucket2/db/t/, false",
    "s3://bucket/db/t/, gs://bucket/db/t/, false",
    "s3://bucket/db/t/, s3://bucket/db/T/, false",
    "abfs://c@account.dfs.core.windows.net/t/, abfs://c2@account.dfs.core.windows.net/t/, false",
    "abfs://c@account.dfs.core.windows.net/t/, abfs://c@account2.dfs.core.windows.net/t/, false",
    "file:///tmp/wh/t/, file:///tmp/wh/t2/, false",
  })
  public void equivalentBaseLocations(String one, String two, boolean equivalent) {
    SharedBaseLocations shared =
        new SharedBaseLocations(
            Map.of("a", Set.of(StorageUri.of(one)), "b", Set.of(StorageUri.of(two))));

    soft.assertThat(shared.isShared(StorageUri.of(one))).isEqualTo(equivalent);
    soft.assertThat(shared.isShared(StorageUri.of(two))).isEqualTo(equivalent);
    soft.assertThat(shared.contentIds(StorageUri.of(one)))
        .containsExactlyElementsOf(equivalent ? List.of("a", "b") : List.of("a"));
  }
}
