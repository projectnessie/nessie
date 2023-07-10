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
package org.projectnessie.restcatalog.service.resources;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;

import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.restcatalog.service.DecodedPrefix;

@ExtendWith(SoftAssertionsExtension.class)
public class TestBaseIcebergResource {

  public static final String DEFAULT_WAREHOUSE = "my-warehouse";
  public static final ParsedReference DEFAULT_REFERENCE =
      parsedReference("default-ref-name", "deadbeef", ReferenceType.BRANCH);
  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<Arguments> decodePrefix() {
    return Stream.of(
        arguments(null, DEFAULT_REFERENCE, DEFAULT_WAREHOUSE),
        arguments("", DEFAULT_REFERENCE, DEFAULT_WAREHOUSE),
        arguments("|", DEFAULT_REFERENCE, DEFAULT_WAREHOUSE),
        arguments("|datalake", DEFAULT_REFERENCE, "datalake"),
        arguments("main", parsedReference("main", null, null), DEFAULT_WAREHOUSE),
        arguments("main@", parsedReference("main", null, null), DEFAULT_WAREHOUSE),
        arguments("main|", parsedReference("main", null, null), DEFAULT_WAREHOUSE),
        arguments("main@|", parsedReference("main", null, null), DEFAULT_WAREHOUSE),
        arguments("main|datalake", parsedReference("main", null, null), "datalake"),
        arguments("foo@feedbeef|datalake", parsedReference("foo", "feedbeef", null), "datalake"),
        arguments("@feedbeef|datalake", parsedReference(null, "feedbeef", null), "datalake"));
  }

  @ParameterizedTest
  @MethodSource
  void decodePrefix(String prefix, ParsedReference expectedRef, String expectedWarehouse) {
    soft.assertThat(
            BaseIcebergResource.decodePrefix(
                prefix, DEFAULT_REFERENCE, DEFAULT_WAREHOUSE, Assertions::fail))
        .isEqualTo(DecodedPrefix.decodedPrefix(expectedRef, expectedWarehouse));
  }
}
