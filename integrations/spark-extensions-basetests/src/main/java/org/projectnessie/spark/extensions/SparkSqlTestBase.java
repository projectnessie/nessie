/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.spark.extensions;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.execution.datasources.v2.CatalogBridge;
import org.apache.spark.sql.execution.datasources.v2.CatalogUtils;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.config.NessieClientConfigSource;
import org.projectnessie.client.config.NessieClientConfigSources;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public abstract class SparkSqlTestBase {

  protected static final String NON_NESSIE_CATALOG = "invalid_hive";
  protected static final SparkConf conf = new SparkConf();

  protected static SparkSession spark;

  protected Branch mainBeforeTest;
  protected Branch initialDefaultBranch;

  protected String refName;
  protected String additionalRefName;
  protected NessieApiV2 api;

  protected String nessieApiUri() {
    return format(
        "%s/api/v2",
        requireNonNull(
            System.getProperty("quarkus.http.test-url"),
            "Required system property quarkus.http.test-url is not set"));
  }

  protected String icebergApiUri() {
    return format(
        "%s/iceberg/",
        requireNonNull(
            System.getProperty("quarkus.http.test-url"),
            "Required system property quarkus.http.test-url is not set"));
  }

  protected abstract String warehouseURI();

  protected Map<String, String> sparkHadoop() {
    return emptyMap();
  }

  protected boolean useIcebergREST() {
    return false;
  }

  protected Map<String, String> nessieParams() {
    if (useIcebergREST()) {
      return ImmutableMap.of(
          "uri", format("%s%s", icebergApiUri(), defaultBranch()), "type", "rest");
    }

    return ImmutableMap.of(
        "ref",
        defaultBranch(),
        "uri",
        nessieApiUri(),
        "client-api-version",
        "2",
        "warehouse",
        warehouseURI(),
        "catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog");
  }

  protected boolean requiresCommonAncestor() {
    return false;
  }

  @BeforeEach
  protected void setupSparkAndApi(TestInfo testInfo)
      throws NessieNotFoundException, NessieConflictException {
    api =
        NessieClientBuilder.createClientBuilderFromSystemSettings(nessieClientConfigSource())
            .withUri(nessieApiUri())
            .build(NessieApiV2.class);

    refName = testInfo.getTestMethod().map(Method::getName).get();
    additionalRefName = refName + "_other";

    mainBeforeTest = initialDefaultBranch = api.getDefaultBranch();

    if (requiresCommonAncestor()) {
      initialDefaultBranch =
          api.commitMultipleOperations()
              .branch(initialDefaultBranch)
              .commitMeta(CommitMeta.fromMessage("INFRA: initial commit"))
              .operation(Put.of(ContentKey.of("dummy"), IcebergTable.of("foo", 1, 2, 3, 4)))
              .commit();
      initialDefaultBranch =
          api.commitMultipleOperations()
              .branch(initialDefaultBranch)
              .commitMeta(CommitMeta.fromMessage("INFRA: common ancestor"))
              .operation(Delete.of(ContentKey.of("dummy")))
              .commit();
    }

    sparkHadoop().forEach((k, v) -> conf.set(format("spark.hadoop.%s", k), v));

    nessieParams()
        .forEach(
            (k, v) -> {
              conf.set(format("spark.sql.catalog.nessie.%s", k), v);
              conf.set(format("spark.sql.catalog.spark_catalog.%s", k), v);
            });

    conf.set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .set("spark.ui.enabled", "false")
        .set("spark.testing", "true")
        .set("spark.sql.warehouse.dir", warehouseURI())
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog");

    // the following catalog is only added to test a check in the nessie spark extensions
    conf.set(
            format("spark.sql.catalog.%s", NON_NESSIE_CATALOG),
            "org.apache.iceberg.spark.SparkCatalog")
        .set(
            format("spark.sql.catalog.%s.catalog-impl", NON_NESSIE_CATALOG),
            "org.apache.iceberg.hive.HiveCatalog");

    spark = SparkSession.builder().master("local[2]").config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  protected NessieClientConfigSource nessieClientConfigSource() {
    return NessieClientConfigSources.defaultConfigSources();
  }

  @AfterEach
  protected void removeBranches() throws NessieConflictException, NessieNotFoundException {
    @SuppressWarnings("resource")
    CatalogPlugin sparkCatalog =
        SparkSession.active().sessionState().catalogManager().catalog("nessie");
    try (CatalogBridge bridge = CatalogUtils.buildBridge(sparkCatalog, "nessie")) {
      bridge.setCurrentRefForSpark(Branch.of(defaultBranch(), null), false);
    }

    if (api != null) {
      Branch defaultBranch = api.getDefaultBranch();
      for (Reference ref : api.getAllReferences().get().getReferences()) {
        if (ref instanceof Tag
            || (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName()))) {
          api.deleteReference().reference(ref).delete();
        }
      }
      api.assignReference().assignTo(mainBeforeTest).reference(defaultBranch).assign();
      api.close();
      api = null;
    }
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  protected String defaultBranch() {
    return initialDefaultBranch.getName();
  }

  protected String defaultHash() {
    return initialDefaultBranch.getHash();
  }

  @FormatMethod
  protected static List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }

    return rows.stream().map(SparkSqlTestBase::toJava).collect(Collectors.toList());
  }

  @FormatMethod
  protected static List<Object[]> sqlWithEmptyCache(String query, Object... args) {
    try (SparkSession sparkWithEmptyCache = spark.cloneSession()) {
      List<Row> rows = sparkWithEmptyCache.sql(format(query, args)).collectAsList();
      return rows.stream().map(SparkSqlTestBase::toJava).collect(Collectors.toList());
    }
  }

  protected static Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  /**
   * This looks weird but it gives a clear semantic way to turn a list of objects into a 'row' for
   * spark assertions.
   */
  protected static Object[] row(Object... values) {
    return values;
  }

  List<SparkCommitLogEntry> fetchLog(String branch) {
    return sql("SHOW LOG %s IN nessie", branch).stream()
        .map(SparkCommitLogEntry::fromShowLog)
        .collect(Collectors.toList());
  }

  protected void createBranchForTest(String branchName) throws NessieNotFoundException {
    assertThat(
            sql("CREATE BRANCH %s IN nessie FROM %s", branchName, initialDefaultBranch.getName()))
        .containsExactly(row("Branch", branchName, defaultHash()));
    assertThat(api.getReference().refName(branchName).get())
        .isEqualTo(Branch.of(branchName, defaultHash()));
  }

  protected void createTagForTest(String tagName) throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie FROM %s", tagName, initialDefaultBranch.getName()))
        .containsExactly(row("Tag", tagName, defaultHash()));
    assertThat(api.getReference().refName(tagName).get()).isEqualTo(Tag.of(tagName, defaultHash()));
  }

  List<SparkCommitLogEntry> createBranchCommitAndReturnLog()
      throws NessieConflictException, NessieNotFoundException {
    createBranchForTest(refName);
    return commitAndReturnLog(refName, defaultHash());
  }

  List<SparkCommitLogEntry> commitAndReturnLog(String branch, String initialHashOrBranch)
      throws NessieNotFoundException, NessieConflictException {
    ContentKey key = ContentKey.of("table", "name");
    CommitMeta cm1 =
        ImmutableCommitMeta.builder()
            .author("sue")
            .authorTime(Instant.ofEpochMilli(1))
            .message("1")
            .putProperties("test", "123")
            .build();

    CommitMeta cm2 =
        ImmutableCommitMeta.builder()
            .author("janet")
            .authorTime(Instant.ofEpochMilli(10))
            .message("2")
            .putProperties("test", "123")
            .build();

    CommitMeta cm3 =
        ImmutableCommitMeta.builder()
            .author("alice")
            .authorTime(Instant.ofEpochMilli(100))
            .message("3")
            .putProperties("test", "123")
            .build();

    Content content =
        api.getContent().refName(branch).hashOnRef(initialHashOrBranch).key(key).get().get(key);

    Operations ops =
        content != null
            ? ImmutableOperations.builder()
                .addOperations(Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42, content.getId())))
                .commitMeta(cm1)
                .build()
            : ImmutableOperations.builder()
                .addOperations(
                    Put.of(ContentKey.of("table"), Namespace.of("table")),
                    Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42)))
                .commitMeta(cm1)
                .build();

    Branch ref1 =
        api.commitMultipleOperations()
            .branchName(branch)
            .hash(initialHashOrBranch)
            .operations(ops.getOperations())
            .commitMeta(ops.getCommitMeta())
            .commit();

    content = api.getContent().reference(ref1).key(key).get().get(key);

    Operations ops2 =
        ImmutableOperations.builder()
            .addOperations(Put.of(key, IcebergTable.of("bar", 42, 42, 42, 42, content.getId())))
            .commitMeta(cm2)
            .build();

    Branch ref2 =
        api.commitMultipleOperations()
            .branchName(branch)
            .hash(ref1.getHash())
            .operations(ops2.getOperations())
            .commitMeta(ops2.getCommitMeta())
            .commit();

    content = api.getContent().reference(ref2).key(key).get().get(key);

    Operations ops3 =
        ImmutableOperations.builder()
            .addOperations(Put.of(key, IcebergTable.of("baz", 42, 42, 42, 42, content.getId())))
            .commitMeta(cm3)
            .build();

    Branch ref3 =
        api.commitMultipleOperations()
            .branchName(branch)
            .hash(ref2.getHash())
            .operations(ops3.getOperations())
            .commitMeta(ops3.getCommitMeta())
            .commit();

    List<SparkCommitLogEntry> resultList = new ArrayList<>();
    resultList.add(SparkCommitLogEntry.fromCommitMeta(cm3, ref3.getHash()));
    resultList.add(SparkCommitLogEntry.fromCommitMeta(cm2, ref2.getHash()));
    resultList.add(SparkCommitLogEntry.fromCommitMeta(cm1, ref1.getHash()));
    return resultList;
  }
}
