/*
 * Copyright (C) 2024 Dremio
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
import static org.projectnessie.catalog.service.rest.TableRef.tableRef;

import java.net.URI;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

@ExtendWith(SoftAssertionsExtension.class)
public class TestExternalBaseUri {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void resolveTableFromUri(String baseUri, String location, Optional<TableRef> expected) {
    ExternalBaseUri externalBaseUri = () -> URI.create(baseUri);

    Optional<TableRef> actual = externalBaseUri.resolveTableFromUri(location);
    soft.assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> resolveTableFromUri() {
    return Stream.of(
        arguments(
            "http://127.0.0.1:42003/",
            "http://127.0.0.1:42003/catalog/v1/trees/main@447b97b0dd76aca20ba1c18b57f77f269d0711c3fe584eac7ab44dcf52ac5d54/snapshot/newdb.table?format=iceberg_imported",
            Optional.of(
                tableRef(
                    ContentKey.fromPathString("newdb.table"),
                    ParsedReference.parsedReference(
                        "main",
                        "447b97b0dd76aca20ba1c18b57f77f269d0711c3fe584eac7ab44dcf52ac5d54",
                        Reference.ReferenceType.BRANCH),
                    null))),
        // The actual "root URI" does not matter for resolveTableFromUri(), there's
        // isNessieCatalogUri() to check for that.
        arguments(
            "http://127.0.0.1:10000/",
            "http://127.0.0.1:42003/catalog/v1/trees/main@447b97b0dd76aca20ba1c18b57f77f269d0711c3fe584eac7ab44dcf52ac5d54/snapshot/newdb.table?format=iceberg_imported",
            Optional.of(
                tableRef(
                    ContentKey.fromPathString("newdb.table"),
                    ParsedReference.parsedReference(
                        "main",
                        "447b97b0dd76aca20ba1c18b57f77f269d0711c3fe584eac7ab44dcf52ac5d54",
                        Reference.ReferenceType.BRANCH),
                    null))),
        arguments("http://127.0.0.1:10000/", "s3://foo/bar", Optional.empty()));
  }
}
