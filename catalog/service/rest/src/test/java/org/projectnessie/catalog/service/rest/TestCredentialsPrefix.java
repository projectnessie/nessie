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
package org.projectnessie.catalog.service.rest;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.catalog.service.rest.IcebergApiV1ResourceBase.SEPARATOR;
import static org.projectnessie.catalog.service.rest.IcebergApiV1TableResource.credentialsPrefix;
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCredentialsPrefix {
  @InjectSoftAssertions protected SoftAssertions soft;

  static final String HEAD = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

  @ParameterizedTest
  @MethodSource
  public void credentialsPrefixes(
      ParsedReference requested,
      String warehouse,
      String expectedPrefix,
      String expectedName,
      String expectedHash) {
    String prefix =
        credentialsPrefix(
            tableRef(ContentKey.of("ns", "table"), requested, warehouse),
            Branch.of(requested.name(), HEAD));

    soft.assertThat(prefix).isEqualTo(expectedPrefix);

    String refPart = prefix.replace(SEPARATOR, '/').split("\\|", 2)[0];
    ParsedReference decoded = resolveReferencePathElement(refPart, BRANCH, () -> "main");
    soft.assertThat(decoded.name()).isEqualTo(expectedName);
    soft.assertThat(decoded.hashWithRelativeSpec()).isEqualTo(expectedHash);
  }

  static Stream<Arguments> credentialsPrefixes() {
    return Stream.of(
        arguments(parsedReference("main", null, BRANCH), "wh", "main|wh", "main", null),
        arguments(parsedReference("main", null, BRANCH), null, "main", "main", null),
        arguments(
            parsedReference("feature/x", null, BRANCH),
            "wh",
            "feature" + SEPARATOR + "x@|wh",
            "feature/x",
            null),
        arguments(
            parsedReference("main", "12345678", BRANCH),
            "wh",
            "main@" + HEAD + "|wh",
            "main",
            HEAD),
        arguments(
            parsedReference("main", "*2026-01-01T00:00:00Z", BRANCH),
            "wh",
            "main@" + HEAD + "|wh",
            "main",
            HEAD),
        arguments(
            parsedReference("feature/x", "12345678", BRANCH),
            "wh",
            "feature" + SEPARATOR + "x@" + HEAD + "|wh",
            "feature/x",
            HEAD));
  }
}
