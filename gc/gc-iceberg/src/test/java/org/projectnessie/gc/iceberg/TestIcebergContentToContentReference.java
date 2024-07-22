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
package org.projectnessie.gc.iceberg;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.UDF;

public class TestIcebergContentToContentReference {

  static Stream<Content> nonIcebergTable() {
    return Stream.of(
        ImmutableDeltaLakeTable.builder().id("123").lastCheckpoint("lc").build(),
        UDF.udf("udf-meta", "42", "666"),
        Namespace.of("foo", "bar"));
  }

  @ParameterizedTest
  @MethodSource("nonIcebergTable")
  public void nonIcebergTable(Content content) {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                IcebergContentToContentReference.INSTANCE.contentToReference(
                    content, "12345678", ContentKey.of("foo", "bar")))
        .withMessageStartingWith("Expect ICEBERG_TABLE/ICEBERG_VIEW, but got " + content.getType());
  }

  @Test
  public void icebergTable() {
    IcebergTable table = IcebergTable.of("meta-1", 42L, 43, 44, 45, "cid");
    assertThat(
            IcebergContentToContentReference.INSTANCE.contentToReference(
                table, "12345678", ContentKey.of("foo", "bar")))
        .extracting(
            ContentReference::contentId,
            ContentReference::commitId,
            ContentReference::contentKey,
            ContentReference::metadataLocation,
            ContentReference::snapshotId)
        .containsExactly("cid", "12345678", ContentKey.of("foo", "bar"), "meta-1", 42L);
  }

  @Test
  public void icebergView() {
    IcebergView table = IcebergView.of("cid", "meta-1", 42L, 43);
    assertThat(
            IcebergContentToContentReference.INSTANCE.contentToReference(
                table, "12345678", ContentKey.of("foo", "bar")))
        .extracting(
            ContentReference::contentId,
            ContentReference::commitId,
            ContentReference::contentKey,
            ContentReference::metadataLocation,
            ContentReference::snapshotId)
        .containsExactly("cid", "12345678", ContentKey.of("foo", "bar"), "meta-1", 42L);
  }
}
