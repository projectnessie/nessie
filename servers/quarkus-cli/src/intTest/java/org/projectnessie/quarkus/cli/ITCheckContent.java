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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.quarkus.cli.BaseCommand.EXIT_CODE_CONTENT_ERROR;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.util.Comparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.testworker.OnRefOnly;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith(NessieCliTestExtension.class)
class ITCheckContent extends BaseContentTest<CheckContentEntry> {

  private static final IcebergTable table1 = IcebergTable.of("meta_111", 1, 2, 3, 4, "111");
  private static final IcebergTable table2 = IcebergTable.of("meta_222", 2, 3, 4, 5, "222");
  private static final IcebergTable table3 = IcebergTable.of("meta_333", 3, 4, 5, 6, "333");
  private static final IcebergTable table4 = IcebergTable.of("meta_444", 4, 5, 6, 7, "444");

  ITCheckContent() {
    super(CheckContentEntry.class);
  }

  @Test
  public void testEmptyRepo(QuarkusMainLauncher launcher) {
    launchNoFile(launcher, "check-content");
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testNonExistingKey(QuarkusMainLauncher launcher) throws Exception {
    launch(launcher, "check-content", "-k", "namespace123", "-k", "unknown12345");
    assertThat(entries)
        .hasSize(1)
        .first()
        .extracting(
            CheckContentEntry::getKey,
            CheckContentEntry::getStatus,
            CheckContentEntry::getContent,
            CheckContentEntry::getErrorMessage)
        .containsExactly(
            ContentKey.of("namespace123", "unknown12345"), "ERROR", null, "Missing content");
    assertThat(result.exitCode()).isEqualTo(EXIT_CODE_CONTENT_ERROR);
  }

  @Test
  public void testAdapterError(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {
    ContentKey k1 = ContentKey.of("table123");
    adapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(BranchName.of("main"))
            .commitMetaSerialized(ByteString.copyFrom(new byte[] {1, 2, 3}))
            .addPuts(
                KeyWithBytes.of(
                    k1,
                    ContentId.of("id123"),
                    (byte) payloadForContent(Content.Type.ICEBERG_TABLE),
                    ByteString.copyFrom(new byte[] {1, 2, 3})))
            .build());

    launch(launcher, "check-content");
    assertThat(entries)
        .hasSize(1)
        .first()
        .extracting(
            CheckContentEntry::getKey,
            CheckContentEntry::getStatus,
            CheckContentEntry::getContent,
            CheckContentEntry::getErrorMessage,
            e ->
                e.getExceptionStackTrace() != null
                    && e.getExceptionStackTrace()
                        .contains("Protocol message contained an invalid tag"))
        .containsExactly(ContentKey.of("table123"), "ERROR", null, "Failure parsing data", true);
    assertThat(result.exitCode()).isEqualTo(EXIT_CODE_CONTENT_ERROR);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 10})
  public void testWorkerError(int batchSize, QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {

    ByteString broken = ByteString.copyFrom(new byte[] {1, 2, 3});
    commit(table1, adapter, broken);
    commit(table2, adapter, broken);
    commit(table3, adapter, broken);
    commit(table4, adapter, broken);

    // Note: SimpleStoreWorker will not be able to parse IcebergTable objects
    launch(launcher, "check-content", "--summary", "--batch=" + batchSize);
    assertThat(entries.stream().sorted(Comparator.comparing(CheckContentEntry::getKey)))
        .extracting(
            CheckContentEntry::getKey,
            CheckContentEntry::getStatus,
            e -> e.getErrorMessage() != null && !e.getErrorMessage().isEmpty(),
            e -> e.getExceptionStackTrace() != null && !e.getExceptionStackTrace().isEmpty())
        .containsExactly(
            tuple(
                ContentKey.of("test_namespace"),
                batchSize > 1 ? "ERROR" : "OK",
                batchSize > 1,
                batchSize > 1),
            tuple(ContentKey.of("test_namespace", "table_111"), "ERROR", true, true),
            tuple(ContentKey.of("test_namespace", "table_222"), "ERROR", true, true),
            tuple(ContentKey.of("test_namespace", "table_333"), "ERROR", true, true),
            tuple(ContentKey.of("test_namespace", "table_444"), "ERROR", true, true));
    assertThat(result.exitCode()).isEqualTo(EXIT_CODE_CONTENT_ERROR);
    assertThat(result.getOutputStream())
        .contains(format("Detected %d errors in 5 keys.", batchSize > 1 ? 5 : 4));
  }

  @Test
  public void testValidData(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {

    commit(table1, adapter);
    commit(table2, adapter);

    launch(launcher, "check-content", "--show-content");
    assertThat(entries.stream().sorted(Comparator.comparing(CheckContentEntry::getKey)))
        .extracting(
            CheckContentEntry::getKey, CheckContentEntry::getStatus, CheckContentEntry::getContent)
        .containsExactly(
            tuple(ContentKey.of("test_namespace"), "OK", namespace),
            tuple(ContentKey.of("test_namespace", "table_111"), "OK", table1),
            tuple(ContentKey.of("test_namespace", "table_222"), "OK", table2));
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testValidDataNoContent(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {
    commit(table1, adapter);
    commit(table2, adapter);

    launch(launcher, "check-content");
    assertThat(entries).hasSize(3);
    assertThat(entries).allSatisfy(e -> assertThat(e.getContent()).isNull());
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testValidDataStdOut(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {
    commit(table1, adapter);
    commit(table2, adapter);

    launchNoFile(
        launcher, "check-content", "-s", "--show-content", "--output", "-"); // '-' for STDOUT
    assertThat(result.getOutputStream()).anySatisfy(line -> assertThat(line).contains("table_111"));
    assertThat(result.getOutputStream()).anySatisfy(line -> assertThat(line).contains("table_222"));
    assertThat(result.getOutputStream()).anySatisfy(line -> assertThat(line).contains("meta_111"));
    assertThat(result.getOutputStream()).anySatisfy(line -> assertThat(line).contains("meta_222"));
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutputStream()).contains("Detected 0 errors in 3 keys.");
  }

  @Test
  public void testErrorOnly(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {
    commit(table1, adapter);
    launch(launcher, "check-content", "--error-only");
    assertThat(entries).hasSize(0);
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testHashWithBrokenCommit(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws Exception {
    commit(table1, adapter);
    ReferenceInfo<?> good = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);

    OnRefOnly val = onRef("123", "222");
    commit(val.getId(), (byte) payloadForContent(val), val.serialized(), adapter);

    launch(launcher, "check-content", "--hash", good.getHash().asString());
    assertThat(entries.stream().sorted(Comparator.comparing(CheckContentEntry::getKey)))
        .extracting(CheckContentEntry::getKey, CheckContentEntry::getStatus)
        .containsExactly(
            tuple(ContentKey.of("test_namespace"), "OK"),
            tuple(ContentKey.of("test_namespace", "table_111"), "OK"));
    assertThat(result.exitCode()).isEqualTo(0);
  }
}
