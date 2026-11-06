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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.storage.uri.StorageUri;

public class TestStoragePrefixes {

  @ParameterizedTest
  @CsvSource({
    // the prefix itself, with and without trailing separators
    "s3://bucket/team_a, s3://bucket/team_a, true",
    "s3://bucket/team_a/, s3://bucket/team_a, true",
    "s3://bucket/team_a, s3://bucket/team_a/, true",
    "s3://bucket/team_a/, s3://bucket/team_a/, true",
    // below the prefix
    "s3://bucket/team_a, s3://bucket/team_a/orders, true",
    "s3://bucket/team_a/, s3://bucket/team_a/orders/, true",
    "s3://bucket/team_a/, s3://bucket/team_a/orders/nested/deeper/, true",
    // only whole path segments match
    "s3://bucket/team_a, s3://bucket/team_ab, false",
    "s3://bucket/team_a/, s3://bucket/team_ab/orders/, false",
    "s3://bucket/team_a/orders, s3://bucket/team_a/orders2/, false",
    // above or beside the prefix
    "s3://bucket/team_a/, s3://bucket/, false",
    "s3://bucket/team_a/, s3://bucket, false",
    "s3://bucket/team_a/orders/, s3://bucket/team_a/, false",
    "s3://bucket/team_a/, s3://bucket/team_b/events/, false",
    // a prefix without path is the whole bucket
    "s3://bucket, s3://bucket/anything/at/all/, true",
    "s3://bucket/, s3://bucket, true",
    "s3://bucket, s3://other/anything/, false",
    // scheme and authority must be equal
    "s3://bucket/team_a/, s3://other/team_a/orders/, false",
    "s3://bucket/team_a/, gs://bucket/team_a/orders/, false",
    "s3://bucket/team_a/, s3a://bucket/team_a/orders/, false",
    // file locations have no authority
    "file:///warehouse/team_a/, file:///warehouse/team_a/orders/, true",
    "file:///warehouse/team_a/, file:///warehouse/team_ab/, false",
  })
  public void isAtOrBelow(String prefix, String location, boolean expected) {
    assertThat(StoragePrefixes.isAtOrBelow(StorageUri.of(prefix), StorageUri.of(location)))
        .describedAs("%s is at or below %s", location, prefix)
        .isEqualTo(expected);
  }

  @Test
  public void anyOfSeveralPrefixes() {
    Predicate<StorageUri> predicate =
        StoragePrefixes.atOrBelowAnyOf(
            List.of(
                StorageUri.of("s3://bucket/team_a/"), StorageUri.of("s3://bucket/team_c/logs")));

    assertThat(predicate)
        .accepts(
            StorageUri.of("s3://bucket/team_a/orders/"),
            StorageUri.of("s3://bucket/team_c/logs/"),
            StorageUri.of("s3://bucket/team_c/logs/2026/"))
        .rejects(
            StorageUri.of("s3://bucket/team_b/events/"),
            StorageUri.of("s3://bucket/team_c/"),
            StorageUri.of("s3://bucket/team_c/logs2/"));
  }

  @Test
  public void noPrefixesAcceptNothing() {
    assertThat(StoragePrefixes.atOrBelowAnyOf(List.of()))
        .rejects(StorageUri.of("s3://bucket/anything/"));
  }
}
