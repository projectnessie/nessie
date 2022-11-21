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

import com.google.protobuf.ByteString;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith(NessieCliTestExtension.class)
class ITContentInfo extends BaseContentTest<ContentInfoEntry> {

  private static final IcebergTable table1 = IcebergTable.of("meta_111", 1, 2, 3, 4, "111");
  private static final IcebergTable table2 = IcebergTable.of("meta_222", 2, 3, 4, 5, "222");
  private static final IcebergTable table3 = IcebergTable.of("meta_333", 2, 3, 4, 5, "333");

  ITContentInfo() {
    super(ContentInfoEntry.class);
  }

  @Test
  public void testEmptyRepo(QuarkusMainLauncher launcher) {
    launchNoFile(launcher, "content-info");
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testNonExistingKey(QuarkusMainLauncher launcher) throws Exception {
    launch(launcher, "content-info", "-k", "namespace123", "-k", "unknown12345");
    assertThat(entries).hasSize(1);
    assertThat(entries)
        .first()
        .extracting(ContentInfoEntry::getKey, ContentInfoEntry::getErrorMessage)
        .containsExactly(ContentKey.of("namespace123", "unknown12345"), "Missing content");
    assertThat(result.exitCode()).isEqualTo(3);
  }

  @Test
  public void testDetached(QuarkusMainLauncher launcher, DatabaseAdapter adapter) throws Exception {
    commit(table1, adapter);

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    launch(launcher, "content-info", "--hash", main.getHash().asString());
    assertThat(entries).hasSize(1);
    assertThat(entries)
        .first()
        .extracting(
            ContentInfoEntry::getKey,
            ContentInfoEntry::getReference,
            ContentInfoEntry::getDistanceFromHead,
            ContentInfoEntry::getHash)
        .containsExactly(
            ContentKey.of("test_namespace", "table_111"),
            "DETACHED",
            0L,
            main.getHash().asString());
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 100})
  public void testValidData(int batchSize, QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {

    commit(table1, adapter);
    commit(table2, adapter);
    commit(table3, adapter);

    launch(launcher, "content-info", "--batch", "" + batchSize);
    assertThat(entries).hasSize(3);
    assertThat(entries).allSatisfy(e -> assertThat(e.getStorageModel()).isEqualTo("ON_REF_STATE"));
    assertThat(entries)
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_111"));
              assertThat(e.getDistanceFromRoot()).isEqualTo(1);
              assertThat(e.getDistanceFromHead()).isEqualTo(2);
            })
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_222"));
              assertThat(e.getDistanceFromRoot()).isEqualTo(2);
              assertThat(e.getDistanceFromHead()).isEqualTo(1);
            })
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_333"));
              assertThat(e.getDistanceFromRoot()).isEqualTo(3);
              assertThat(e.getDistanceFromHead()).isEqualTo(0);
            });
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testGlobalState(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {

    ByteString emptyContent = ObjectTypes.Content.newBuilder().build().toByteString();

    commit(table1, adapter);
    commit(table2, adapter, emptyContent);

    launch(launcher, "content-info", "--summary");
    assertThat(entries).hasSize(2);
    assertThat(entries)
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_111"));
              assertThat(e.getStorageModel()).isEqualTo("ON_REF_STATE");
              assertThat(e.getReference()).isEqualTo("main");
            })
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_222"));
              assertThat(e.getStorageModel()).isEqualTo("GLOBAL_STATE");
              assertThat(e.getReference()).isEqualTo("main");
            });
    assertThat(result.exitCode()).isEqualTo(0);

    assertThat(result.getOutput())
        .contains("Processed 2 keys: 1 entries have global state; 0 missing entries.");
  }

  @Test
  public void testInvalidData(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {

    ByteString invalidContent = ByteString.copyFrom(new byte[] {1, 2, 3});

    commit(table1, adapter);
    commit(table2, adapter, invalidContent);

    launch(launcher, "content-info");
    assertThat(entries).hasSize(2);
    assertThat(entries)
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_111"));
              assertThat(e.getType()).isEqualTo(Content.Type.ICEBERG_TABLE);
              assertThat(e.getStorageModel()).isEqualTo("ON_REF_STATE");
              assertThat(e.getErrorMessage()).isNull();
            })
        .anySatisfy(
            e -> {
              assertThat(e.getKey()).isEqualTo(ContentKey.of("test_namespace", "table_222"));
              assertThat(e.getType()).isEqualTo(Content.Type.ICEBERG_TABLE);
              assertThat(e.getStorageModel()).isEqualTo("UNKNOWN");
              assertThat(e.getErrorMessage()).isEqualTo("Failure parsing data");
              assertThat(e.getExceptionStackTrace())
                  .contains("Protocol message contained an invalid tag");
            });
    assertThat(result.exitCode()).isEqualTo(0);
  }
}
