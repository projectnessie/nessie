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
package org.projectnessie.gc.iceberg.inttest;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.projectnessie.gc.iceberg.inttest.AbstractITSparkIcebergNessieObjectStorage.Step.dml;
import static org.projectnessie.gc.iceberg.inttest.AbstractITSparkIcebergNessieObjectStorage.Step.expiredDdl;
import static org.projectnessie.gc.iceberg.inttest.AbstractITSparkIcebergNessieObjectStorage.Step.expiredDml;
import static org.projectnessie.gc.iceberg.inttest.AbstractITSparkIcebergNessieObjectStorage.TestCase.testCase;
import static org.projectnessie.gc.iceberg.inttest.Util.expire;
import static org.projectnessie.gc.iceberg.inttest.Util.identifyLiveContents;
import static org.projectnessie.gc.identify.CutoffPolicy.numCommits;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.immutables.value.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.NessieFileIOException;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import org.projectnessie.gc.repository.NessieRepositoryConnector;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.spark.extensions.NessieSparkSessionExtensions;
import org.projectnessie.spark.extensions.SparkSqlTestBase;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractITSparkIcebergNessieObjectStorage extends SparkSqlTestBase {

  @InjectSoftAssertions protected SoftAssertions soft;

  @BeforeAll
  protected static void useNessieExtensions() {
    conf.set("spark.sql.extensions", NessieSparkSessionExtensions.class.getCanonicalName());
  }

  @Value.Immutable
  interface Step {
    static Step dml(String sql) {
      return dml(null, sql);
    }

    static Step expiredDdl(String sql) {
      return expiredDdl(null, sql);
    }

    static Step expiredDml(String sql) {
      return expiredDml(null, sql);
    }

    static Step dml(String branch, String sql) {
      return ImmutableStep.of(branch, sql, false, false);
    }

    static Step expiredDdl(String branch, String sql) {
      return ImmutableStep.of(branch, sql, true, false);
    }

    static Step expiredDml(String branch, String sql) {
      return ImmutableStep.of(branch, sql, true, true);
    }

    @Value.Parameter(order = 1)
    @Nullable
    String branch();

    @Value.Parameter(order = 2)
    String sql();

    @Value.Parameter(order = 3)
    boolean expired();

    @Value.Parameter(order = 4)
    boolean expireManifestList();
  }

  @Value.Immutable
  interface TestCase {
    static ImmutableTestCase.Builder testCase() {
      return ImmutableTestCase.builder();
    }

    List<Step> steps();

    Map<String, CutoffPolicy> policies();

    String namespace();

    @Value.Default
    default Set<Storage> compatibleStorages() {
      return EnumSet.allOf(Storage.class);
    }
  }

  static Stream<TestCase> testCases() {
    return Stream.of(
        // 1
        testCase()
            .namespace("tc_1")
            .addSteps(expiredDdl("CREATE TABLE nessie.tc_1.tbl_a (id int, name string)"))
            .addSteps(expiredDml("INSERT INTO nessie.tc_1.tbl_a select 23, \"test\""))
            .addSteps(dml("INSERT INTO nessie.tc_1.tbl_a select 24, \"case\""))
            .addSteps(expiredDdl("CREATE TABLE nessie.tc_1.tbl_b (id int, name string)"))
            .addSteps(dml("INSERT INTO nessie.tc_1.tbl_b select 42, \"foo\""))
            .putPolicies("main", numCommits(1))
            .build(),
        // 2
        testCase()
            .namespace("tc_2")
            .addSteps(expiredDdl("CREATE TABLE nessie.tc_2.tbl_a (id int, name string)"))
            .addSteps(dml("INSERT INTO nessie.tc_2.tbl_a select 23, \"test\""))
            // on branch "branch_tc_2"
            .addSteps(
                expiredDdl("branch_tc_2", "CREATE TABLE nessie.tc_2.tbl_b (id int, name string)"))
            .addSteps(dml("branch_tc_2", "INSERT INTO nessie.tc_2.tbl_b select 42, \"foo\""))
            // back on "main"
            .addSteps(dml("INSERT INTO nessie.tc_2.tbl_a select 24, \"case\""))
            .putPolicies("main", numCommits(1))
            .putPolicies("branch_tc_2", numCommits(1))
            .build(),
        // 3
        testCase()
            .namespace("tc_3")
            // ADLS FileIO does not support special characters in column names
            .addCompatibleStorages(Storage.S3, Storage.GCS)
            .addSteps(
                expiredDdl(
                    // Note: intentional special chars in the column name
                    // TODO: debug why `#` breaks this test
                    "CREATE TABLE nessie.tc_3.tbl_a (`id\"~!@$%^&*()/` int, name string) "
                        + "PARTITIONED BY (`id\"~!@$%^&*()/`)"))
            .addSteps(expiredDml("INSERT INTO nessie.tc_3.tbl_a select 23, \"test\""))
            .addSteps(dml("INSERT INTO nessie.tc_3.tbl_a select 24, \"case\""))
            .putPolicies("main", numCommits(1))
            .build());
  }

  abstract IcebergFiles icebergFiles();

  @ParameterizedTest
  @MethodSource("testCases")
  public void roundTrips(TestCase testCase) throws Exception {

    assumeThat(testCase.compatibleStorages()).contains(storage());

    api.createNamespace()
        .namespace(testCase.namespace())
        .refName(api.getConfig().getDefaultBranch())
        .create();

    try (IcebergFiles icebergFiles = icebergFiles()) {

      // Tweak Hadoop... s3 vs s3a... oh my...
      spark
          .sparkContext()
          .hadoopConfiguration()
          .set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

      Set<StorageUri> expiredSnapshots = new HashSet<>();
      Set<StorageUri> survivingSnapshots = new HashSet<>();
      int expectedDeletes = 0;

      Set<String> branches = new HashSet<>();
      String currentBranch = initialDefaultBranch.getName();
      branches.add(currentBranch);
      for (Step step : testCase.steps()) {
        String branch = step.branch();
        if (branch == null) {
          branch = initialDefaultBranch.getName();
        }
        if (branches.add(branch)) {
          sql("CREATE BRANCH %s IN nessie", branch);
        }
        if (!branch.equals(currentBranch)) {
          sql("USE REFERENCE %s IN nessie", branch);
          currentBranch = branch;
        }

        sql("%s", step.sql());

        IcebergTable icebergTable = icebergTableFromLastCommit(branch);
        (step.expired() ? expiredSnapshots : survivingSnapshots)
            .add(StorageUri.of(icebergTable.getMetadataLocation()));
        if (step.expired()) {
          expectedDeletes++;
        }
        if (step.expireManifestList()) {
          expectedDeletes++;
        }
      }

      Set<StorageUri> filesBefore = allFiles(icebergFiles);

      PerRefCutoffPolicySupplier cutOffPolicySupplier =
          ref -> testCase.policies().getOrDefault(ref.getName(), CutoffPolicy.NONE);

      Instant maxFileModificationTime = Instant.now();

      // Mark...
      LiveContentSet liveContentSet =
          identifyLiveContents(
              new InMemoryPersistenceSpi(),
              cutOffPolicySupplier,
              NessieRepositoryConnector.nessie(api));
      // ... and sweep
      DeleteSummary deleteSummary = expire(icebergFiles, liveContentSet, maxFileModificationTime);

      Set<StorageUri> filesAfter = allFiles(icebergFiles);
      Set<StorageUri> removedFiles = new TreeSet<>(filesBefore);
      removedFiles.removeAll(filesAfter);

      soft.assertThat(removedFiles).hasSize(expectedDeletes).containsAll(expiredSnapshots);

      soft.assertThat(filesAfter)
          .hasSize(filesBefore.size() - expectedDeletes)
          .containsAll(survivingSnapshots)
          .doesNotContainAnyElementsOf(expiredSnapshots);

      soft.assertThat(deleteSummary)
          .extracting(DeleteSummary::deleted, DeleteSummary::failures)
          .containsExactly((long) expectedDeletes, 0L);

      // Run GC another time, but this time assuming that all commits are live. This triggers a
      // read-attempt against a previously deleted table-metadata, which is equal to "no files
      // from this snapshot". Note: this depends on the cutoff-policies from the test-case.

      // Mark...
      liveContentSet =
          identifyLiveContents(
              new InMemoryPersistenceSpi(),
              ref -> CutoffPolicy.NONE,
              NessieRepositoryConnector.nessie(api));
      // ... and sweep
      deleteSummary = expire(icebergFiles, liveContentSet, maxFileModificationTime);

      soft.assertThat(deleteSummary)
          .extracting(DeleteSummary::deleted, DeleteSummary::failures)
          .containsExactly(0L, 0L);
    }
  }

  private IcebergTable icebergTableFromLastCommit(String branch) throws NessieNotFoundException {
    LogResponse log = api.getCommitLog().refName(branch).maxRecords(1).fetch(FetchOption.ALL).get();
    LogEntry entry = log.getLogEntries().get(0);
    Operation.Put put = (Operation.Put) entry.getOperations().get(0);
    return (IcebergTable) put.getContent();
  }

  protected Set<StorageUri> allFiles(IcebergFiles icebergFiles) throws NessieFileIOException {
    try (Stream<FileReference> list = icebergFiles.listRecursively(bucketUri())) {
      return list.map(FileReference::absolutePath).collect(Collectors.toCollection(TreeSet::new));
    }
  }

  protected abstract StorageUri bucketUri();

  abstract Storage storage();

  enum Storage {
    S3,
    GCS,
    ADLS,
  }
}
