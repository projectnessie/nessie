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
package org.projectnessie.quarkus.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.NAMESPACE;

import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.versioned.Hash;

@QuarkusMainTest
abstract class AbstractContentInfoTests {

  private static final UUID CID_1 = UUID.randomUUID();
  private static final UUID CID_2 = UUID.randomUUID();
  private static final UUID CID_3 = UUID.randomUUID();

  private static final IcebergTable table1 =
      IcebergTable.of("meta_111", 1, 2, 3, 4, CID_1.toString());
  private static final IcebergTable table2 =
      IcebergTable.of("meta_222", 2, 3, 4, 5, CID_2.toString());
  private static final IcebergTable table3 =
      IcebergTable.of("meta_333", 3, 4, 5, 6, CID_3.toString());

  private final BaseContentTest<ContentInfoEntry> outer;

  AbstractContentInfoTests(BaseContentTest<ContentInfoEntry> outer) {
    this.outer = outer;
  }

  @Test
  public void testEmptyRepo(QuarkusMainLauncher launcher) {
    outer.launchNoFile(launcher, "content-info");
    assertThat(outer.result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testNonExistingKey(QuarkusMainLauncher launcher) throws Exception {
    outer.launch(launcher, "content-info", "-k", "namespace123", "-k", "unknown12345");
    assertThat(outer.entries).hasSize(1);
    assertThat(outer.entries)
        .first()
        .extracting(ContentInfoEntry::getKey, ContentInfoEntry::getErrorMessage)
        .containsExactly(ContentKey.of("namespace123", "unknown12345"), "Missing content");
    assertThat(outer.result.exitCode()).isEqualTo(3);
  }

  @Test
  public void testDetached(QuarkusMainLauncher launcher) throws Exception {
    outer.commit(table1);

    Hash head = outer.getMainHead();
    outer.launch(launcher, "content-info", "--hash", head.asString());
    assertThat(outer.entries).hasSize(2);
    assertThat(outer.entries.stream())
        .extracting(
            ContentInfoEntry::getKey,
            ContentInfoEntry::getReference,
            ContentInfoEntry::getDistanceFromHead,
            ContentInfoEntry::getHash)
        .containsExactlyInAnyOrder(
            tuple(ContentKey.of("test_namespace"), "DETACHED", 0L, head.asString()),
            tuple(
                ContentKey.of("test_namespace", "table_" + CID_1),
                "DETACHED",
                0L,
                head.asString()));
    assertThat(outer.result.exitCode()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 100})
  public void testValidData(int batchSize, QuarkusMainLauncher launcher) throws Exception {

    outer.commit(table1);
    outer.commit(table2);
    outer.commit(table3);

    outer.launch(launcher, "content-info", "--batch", "" + batchSize);
    assertThat(outer.entries.stream())
        .extracting(
            ContentInfoEntry::getKey,
            ContentInfoEntry::getStorageModel,
            ContentInfoEntry::getDistanceFromRoot,
            ContentInfoEntry::getDistanceFromHead)
        .containsExactlyInAnyOrder(
            tuple(ContentKey.of("test_namespace"), "ON_REF_STATE", 1L, 2L),
            tuple(ContentKey.of("test_namespace", "table_" + CID_1), "ON_REF_STATE", 1L, 2L),
            tuple(ContentKey.of("test_namespace", "table_" + CID_2), "ON_REF_STATE", 2L, 1L),
            tuple(ContentKey.of("test_namespace", "table_" + CID_3), "ON_REF_STATE", 3L, 0L));
    assertThat(outer.result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testGlobalState(QuarkusMainLauncher launcher) throws Exception {

    ByteString emptyContent = ObjectTypes.Content.newBuilder().build().toByteString();

    outer.commit(table1);
    outer.commit(table2, emptyContent);

    outer.launch(launcher, "content-info", "--summary");
    assertThat(outer.entries.stream())
        .extracting(
            ContentInfoEntry::getKey,
            ContentInfoEntry::getStorageModel,
            ContentInfoEntry::getReference)
        .containsExactlyInAnyOrder(
            tuple(ContentKey.of("test_namespace"), "ON_REF_STATE", "main"),
            tuple(ContentKey.of("test_namespace", "table_" + CID_1), "ON_REF_STATE", "main"),
            tuple(ContentKey.of("test_namespace", "table_" + CID_2), "GLOBAL_STATE", "main"));
    assertThat(outer.result.exitCode()).isEqualTo(0);

    assertThat(outer.result.getOutput())
        .contains("Processed 3 keys: 1 entries have global state; 0 missing entries.");
  }

  @Test
  public void testInvalidData(QuarkusMainLauncher launcher) throws Exception {

    ByteString invalidContent = ByteString.copyFrom(new byte[] {1, 2, 3});

    outer.commit(table1);
    outer.commit(table2, invalidContent);

    outer.launch(launcher, "content-info");
    assertThat(outer.entries.stream())
        .extracting(
            ContentInfoEntry::getKey,
            ContentInfoEntry::getType,
            ContentInfoEntry::getStorageModel,
            ContentInfoEntry::getErrorMessage,
            e ->
                e.getExceptionStackTrace() != null
                    && e.getExceptionStackTrace()
                        .contains("Protocol message contained an invalid tag"))
        .containsExactlyInAnyOrder(
            tuple(ContentKey.of("test_namespace"), NAMESPACE, "ON_REF_STATE", null, false),
            tuple(
                ContentKey.of("test_namespace", "table_" + CID_1),
                ICEBERG_TABLE,
                "ON_REF_STATE",
                null,
                false),
            tuple(
                ContentKey.of("test_namespace", "table_" + CID_2),
                ICEBERG_TABLE,
                "UNKNOWN",
                "Failure parsing data",
                true));
    assertThat(outer.result.exitCode()).isEqualTo(0);
  }
}
