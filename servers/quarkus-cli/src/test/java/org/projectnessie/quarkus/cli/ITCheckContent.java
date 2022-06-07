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
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.testworker.SimpleStoreWorker;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith(NessieCliTestExtension.class)
class ITCheckContent {

  @Test
  public void testEmptyRepo(QuarkusMainLauncher launcher) {
    LaunchResult result = launcher.launch("check-content");
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testNonExistingKey(QuarkusMainLauncher launcher) {
    LaunchResult result =
        launcher.launch(
            "check-content", "--key=namespace123", "--key=unknown12345", "--separator", "|");
    assertThat(result.getOutputStream())
        .anySatisfy(
            line -> {
              assertThat(line).contains("ERROR");
              assertThat(line).contains("|");
              assertThat(line).contains("namespace123");
              assertThat(line).contains("unknown12345");
              assertThat(line).contains("Missing content");
            });
    assertThat(result.exitCode()).isEqualTo(2);
  }

  @Test
  public void testAdapterError(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Key k1 = Key.of("namespace123", "table123");
    adapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(BranchName.of("main"))
            .commitMetaSerialized(ByteString.copyFrom(new byte[] {1, 2, 3}))
            .addPuts(
                KeyWithBytes.of(
                    k1, ContentId.of("id123"), (byte) 0, ByteString.copyFrom(new byte[] {1, 2, 3})))
            .build());

    LaunchResult result = launcher.launch("check-content");
    assertThat(result.getOutputStream())
        .anySatisfy(
            line -> {
              assertThat(line).contains("ERROR");
              assertThat(line).contains("namespace123");
              assertThat(line).contains("table123");
              assertThat(line).contains("Protocol message contained an invalid tag");
            });
    assertThat(result.exitCode()).isEqualTo(2);
  }

  private void commit(String testId, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    IcebergTable table = IcebergTable.of("meta_" + testId, 1, 2, 3, 4, testId);
    commit(testId, worker.getPayload(table), worker.toStoreOnReferenceState(table), adapter);
  }

  private void commit(String testId, byte payload, ByteString value, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    Key key = Key.of("test_namespace", "table_" + testId);
    ContentId id = ContentId.of(testId);

    adapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(BranchName.of("main"))
            .commitMetaSerialized(
                worker.getMetadataSerializer().toBytes(CommitMeta.fromMessage(testId)))
            .addPuts(KeyWithBytes.of(key, id, payload, value))
            .build());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 10})
  public void testWorkerError(int batchSize, QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {

    commit("111", adapter);
    commit("222", adapter);
    commit("333", adapter);
    commit("444", adapter);

    // Note: SimpleStoreWorker will not be able to parse IcebergTable objects
    LaunchResult result =
        launcher.launch(
            "check-content", "-v", "-B", "" + batchSize, "-W", SimpleStoreWorker.class.getName());
    assertThat(result.getOutputStream()).anyMatch(line -> line.matches("ERROR.+table_111.+"));
    assertThat(result.getOutputStream()).anyMatch(line -> line.matches("ERROR.+table_222.+"));
    assertThat(result.getOutputStream()).anyMatch(line -> line.matches("ERROR.+table_333.+"));
    assertThat(result.getOutputStream()).anyMatch(line -> line.matches("ERROR.+table_444.+"));
    assertThat(result.exitCode()).isEqualTo(2);
  }

  @Test
  public void testValidData(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {

    commit("111", adapter);
    commit("222", adapter);

    LaunchResult result = launcher.launch("check-content", "--show-content");
    assertThat(result.getOutputStream())
        .anyMatch(line -> line.matches("OK:.+test_namespace\\.table_111.+meta_111.*"));
    assertThat(result.getOutputStream())
        .anyMatch(line -> line.matches("OK:.+test_namespace\\.table_222.+meta_222.*"));
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testErrorOnly(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {

    commit("111", adapter);

    LaunchResult result = launcher.launch("check-content", "--error-only");
    assertThat(result.getOutputStream())
        .noneSatisfy(line -> assertThat(line).contains("table_111"));
    assertThat(result.exitCode()).isEqualTo(0);
  }

  @Test
  public void testHashWithBrokenCommit(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException, ReferenceConflictException {

    commit("111", adapter);
    ReferenceInfo<ByteString> good = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);

    commit("222", (byte) 0, ByteString.copyFrom(new byte[] {1, 2, 3}), adapter);

    LaunchResult result =
        launcher.launch("check-content", "-v", "--hash", good.getHash().asString());
    assertThat(result.getOutputStream()).anyMatch(line -> line.matches("OK:.+table_111.+"));
    assertThat(result.getOutputStream()).noneMatch(line -> line.matches(".*table_222.*"));
    assertThat(result.exitCode()).isEqualTo(0);
  }
}
