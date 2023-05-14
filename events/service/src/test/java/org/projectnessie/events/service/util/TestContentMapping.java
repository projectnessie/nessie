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

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.ImmutableContentKey;
import org.projectnessie.events.api.ImmutableDeltaLakeTable;
import org.projectnessie.events.api.ImmutableGenericContent;
import org.projectnessie.events.api.ImmutableIcebergTable;
import org.projectnessie.events.api.ImmutableIcebergView;
import org.projectnessie.events.api.ImmutableNamespace;
import org.projectnessie.events.api.ImmutableUDF;
import org.projectnessie.model.CommitMeta;

class TestContentMapping {

  @Test
  void mapContentKey() {
    org.projectnessie.model.ContentKey input =
        org.projectnessie.model.ImmutableContentKey.builder()
            .elements(Collections.singletonList("foo"))
            .build();
    ContentKey expected = ImmutableContentKey.builder().addElement("foo").build();
    assertThat(ContentMapping.map(input)).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource
  void mapContent(org.projectnessie.model.Content input, Content expected) {
    Content actual = ContentMapping.map(input);
    assertThat(actual).isEqualTo(expected);
  }

  public static Stream<Arguments> mapContent() {
    return Stream.of(
        Arguments.of(
            org.projectnessie.model.ImmutableNamespace.builder()
                .id("id")
                .addElements("foo", "bar")
                .build(),
            ImmutableNamespace.builder().id("id").addElements("foo", "bar").build()),
        Arguments.of(
            org.projectnessie.model.ImmutableIcebergTable.builder()
                .id("id")
                .metadataLocation("metadataLocation")
                .snapshotId(1L)
                .schemaId(2)
                .specId(3)
                .sortOrderId(4)
                .build(),
            ImmutableIcebergTable.builder()
                .id("id")
                .metadataLocation("metadataLocation")
                .snapshotId(1L)
                .schemaId(2)
                .specId(3)
                .sortOrderId(4)
                .build()),
        Arguments.of(
            org.projectnessie.model.ImmutableIcebergView.builder()
                .id("id")
                .metadataLocation("metadataLocation")
                .versionId(1L)
                .schemaId(2)
                .sqlText("sqlText")
                .dialect("dialect")
                .build(),
            ImmutableIcebergView.builder()
                .id("id")
                .metadataLocation("metadataLocation")
                .versionId(1L)
                .schemaId(2)
                .sqlText("sqlText")
                .dialect("dialect")
                .build()),
        Arguments.of(
            org.projectnessie.model.ImmutableIcebergView.builder()
                .id("id")
                .metadataLocation("metadataLocation")
                .versionId(1L)
                .schemaId(2)
                .sqlText("sqlText")
                // no dialect
                .build(),
            ImmutableIcebergView.builder()
                .id("id")
                .metadataLocation("metadataLocation")
                .versionId(1L)
                .schemaId(2)
                .sqlText("sqlText")
                .build()),
        Arguments.of(
            org.projectnessie.model.ImmutableDeltaLakeTable.builder()
                .id("id")
                .addCheckpointLocationHistory("checkpoint")
                .addMetadataLocationHistory("metadata")
                .lastCheckpoint("lastCheckpoint")
                .build(),
            ImmutableDeltaLakeTable.builder()
                .id("id")
                .addCheckpointLocationHistory("checkpoint")
                .addMetadataLocationHistory("metadata")
                .lastCheckpoint("lastCheckpoint")
                .build()),
        Arguments.of(
            org.projectnessie.model.ImmutableUDF.builder()
                .id("id")
                .sqlText("sqlText")
                .dialect("dialect")
                .build(),
            ImmutableUDF.builder().id("id").sqlText("sqlText").dialect("dialect").build()),
        Arguments.of(
            org.projectnessie.model.ImmutableUDF.builder()
                .id("id")
                .sqlText("sqlText")
                // no dialect
                .build(),
            ImmutableUDF.builder().id("id").sqlText("sqlText").build()),
        Arguments.of(
            org.projectnessie.model.types.ImmutableGenericContent.builder()
                .id("id")
                .type(
                    new org.projectnessie.model.Content.Type() {
                      @Override
                      public String name() {
                        return "GENERIC";
                      }

                      @Override
                      public Class<? extends org.projectnessie.model.Content> type() {
                        return org.projectnessie.model.Content.class;
                      }
                    })
                .putAttributes("text", "foo")
                .putAttributes("number", 123)
                .putAttributes("boolean", true)
                .build(),
            ImmutableGenericContent.builder()
                .id("id")
                .genericType("GENERIC")
                .putProperty("text", "foo")
                .putProperty("number", 123)
                .putProperty("boolean", true)
                .build()),
        Arguments.of(
            new MyCustomContent(),
            ImmutableGenericContent.builder()
                .id("id")
                .genericType("MY_CONTENT")
                .putProperty("text", "foo")
                .putProperty("number", 123)
                .putProperty("boolean", true)
                .putProperty(
                    "commitMeta",
                    ImmutableMap.of(
                        "author",
                        "author",
                        "committer",
                        "committer",
                        "message",
                        "message",
                        "hash",
                        "hash",
                        "signedOffBy",
                        "signedOffBy",
                        "authorTime",
                        "1970-01-15T06:56:07.890Z",
                        "commitTime",
                        "1970-01-15T06:56:07.890Z",
                        "properties",
                        Collections.emptyMap(),
                        "authors",
                        Collections.singletonList("author"),
                        "allSignedOffBy",
                        Collections.singletonList("signedOffBy")))
                .build()));
  }

  @SuppressWarnings("unused")
  static class MyCustomContent extends org.projectnessie.model.Content {

    static final Type TYPE =
        new Type() {
          @Override
          public String name() {
            return "MY_CONTENT";
          }

          @Override
          public Class<? extends org.projectnessie.model.Content> type() {
            return MyCustomContent.class;
          }
        };

    @Nullable
    @Override
    public String getId() {
      return "id";
    }

    @Override
    public Type getType() {
      return TYPE;
    }

    public String getText() {
      return "foo";
    }

    public int getNumber() {
      return 123;
    }

    public boolean isBoolean() {
      return true;
    }

    public CommitMeta getCommitMeta() {
      // test nested objects and objects containing lists/maps, Instant, etc.
      return CommitMeta.builder()
          .author("author")
          .committer("committer")
          .message("message")
          .hash("hash")
          .signedOffBy("signedOffBy")
          .authorTime(Instant.ofEpochMilli(1234567890L))
          .commitTime(Instant.ofEpochMilli(1234567890L))
          .build();
    }
  }
}
