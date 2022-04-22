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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

public abstract class AbstractSparkSqlTest {

  @TempDir File tempFile;

  private static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);
  private static final String NON_NESSIE_CATALOG = "invalid_hive";
  protected static SparkConf conf = new SparkConf();

  protected static SparkSession spark;
  protected static String url = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  private String hash;

  private final String refName = "testBranch";
  protected NessieApiV1 api;

  @BeforeEach
  void setupSparkAndApi() throws NessieNotFoundException {
    Map<String, String> nessieParams =
        ImmutableMap.of("ref", "main", "uri", url, "warehouse", tempFile.toURI().toString());

    nessieParams.forEach(
        (k, v) -> {
          conf.set(String.format("spark.sql.catalog.nessie.%s", k), v);
          conf.set(String.format("spark.sql.catalog.spark_catalog.%s", k), v);
        });

    conf.set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .set("spark.testing", "true")
        .set("spark.sql.warehouse.dir", tempFile.toURI().toString())
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog");

    // the following catalog is only added to test a check in the nessie spark extensions
    conf.set(
            String.format("spark.sql.catalog.%s", NON_NESSIE_CATALOG),
            "org.apache.iceberg.spark.SparkCatalog")
        .set(
            String.format("spark.sql.catalog.%s.catalog-impl", NON_NESSIE_CATALOG),
            "org.apache.iceberg.hive.HiveCatalog");

    spark = SparkSession.builder().master("local[2]").config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    api = HttpClientBuilder.builder().withUri(url).build(NessieApiV1.class);
    hash = api.getDefaultBranch().getHash();
  }

  @AfterEach
  void removeBranches() throws NessieConflictException, NessieNotFoundException {
    for (Reference ref : api.getAllReferences().get().getReferences()) {
      if (ref instanceof Branch) {
        api.deleteBranch().branchName(ref.getName()).hash(ref.getHash()).delete();
      }
      if (ref instanceof Tag) {
        api.deleteTag().tagName(ref.getName()).hash(ref.getHash()).delete();
      }
    }
    api.createReference().reference(Branch.of("main", null)).create();
    api.close();
    api = null;
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  protected static void assertEquals(
      String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    assertThat(actualRows).as("%s", context).containsExactlyElementsOf(expectedRows);
  }

  protected static List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rows.stream().map(AbstractSparkSqlTest::toJava).collect(Collectors.toList());
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

  @Test
  public void testRefreshAfterMergeWithIcebergTableCaching() {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .hasSize(1)
        .containsExactly(row("Branch", refName, hash));

    sql("CREATE TABLE nessie.db.tbl (id int, name string)");
    sql("INSERT INTO nessie.db.tbl select 23, \"test\"");
    assertThat(sql("SELECT * FROM nessie.db.tbl")).hasSize(1).containsExactly(row(23, "test"));

    sql(String.format("MERGE BRANCH %s INTO main in nessie", refName));
    assertThat(sql("SELECT * FROM nessie.db.`tbl@main`"))
        .hasSize(1)
        .containsExactly(row(23, "test"));
    assertThat(sql("SELECT * FROM nessie.db.`tbl@%s`", refName))
        .hasSize(1)
        .containsExactly(row(23, "test"));
    sql("INSERT INTO nessie.db.tbl select 24, \"test24\"");
    assertThat(sql("SELECT * FROM nessie.db.`tbl@main`"))
        .hasSize(1)
        .containsExactly(row(23, "test"));
    assertThat(sql("SELECT * FROM nessie.db.tbl"))
        .hasSize(2)
        .containsExactlyInAnyOrder(row(23, "test"), row(24, "test24"));

    // this still sees old data because tbl@testBranch is being cached in Iceberg
    // due to "spark.sql.catalog.nessie.cache-enabled=true" by default
    assertThat(sql("SELECT * FROM nessie.db.`tbl@%s`", refName))
        .hasSize(1)
        .containsExactly(row(23, "test"));

    // using a fresh session with an empty cache sees the data correctly
    assertThat(sqlWithEmptyCache("SELECT * FROM nessie.db.`tbl@%s`", refName))
        .hasSize(2)
        .containsExactlyInAnyOrder(row(23, "test"), row(24, "test24"));
  }

  private static List<Object[]> sqlWithEmptyCache(String query, Object... args) {
    try (SparkSession sparkWithEmptyCache = spark.cloneSession()) {
      List<Row> rows = sparkWithEmptyCache.sql(String.format(query, args)).collectAsList();
      return rows.stream().map(AbstractSparkSqlTest::toJava).collect(Collectors.toList());
    }
  }

  @Test
  void testCreateBranchInExists() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));

    assertThat(sql("CREATE BRANCH IF NOT EXISTS %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));

    assertThatThrownBy(() -> sql("CREATE BRANCH %s IN nessie", refName))
        .isInstanceOf(NessieConflictException.class)
        .hasMessage("Named reference 'testBranch' already exists.");
  }

  @Test
  void testCreateBranch() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));
    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Branch", refName, hash), row("Branch", "main", hash));
  }

  @Test
  void testCreateTag() throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie", refName)).containsExactly(row("Tag", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Tag.of(refName, hash));
    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Tag", refName, hash), row("Branch", "main", hash));
  }

  @Test
  void testCreateReferenceFromHashOnNonDefaultBranch() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie FROM main", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));
    sql("USE REFERENCE %s IN nessie", refName);
    sql("CREATE TABLE nessie.db.tbl (id int, name string)");
    sql("INSERT INTO nessie.db.tbl select 23, \"test\"");
    String newHash =
        api.getCommitLog()
            .refName(refName)
            .maxRecords(1)
            .get()
            .getLogEntries()
            .get(0)
            .getCommitMeta()
            .getHash();

    String tempRef = refName + "_temp";
    sql("CREATE BRANCH %s IN nessie FROM %s", tempRef, refName);
    assertThat(api.getReference().refName(tempRef).get()).isEqualTo(Branch.of(tempRef, newHash));

    String tag = refName + "_temp_tag";
    sql("CREATE TAG %s IN nessie FROM %s", tag, refName);
    assertThat(api.getReference().refName(tag).get()).isEqualTo(Tag.of(tag, newHash));
  }

  @Test
  void testDropBranchIn() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));
    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> api.getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference 'testBranch' not found");
  }

  @Test
  void testDropTagIn() throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie", refName)).containsExactly(row("Tag", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Tag.of(refName, hash));
    assertThat(sql("DROP TAG %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> api.getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference 'testBranch' not found");
  }

  @Test
  void testAssignBranch() throws NessieConflictException, NessieNotFoundException {
    String random = "randomBranch";
    assertThat(sql("CREATE BRANCH %s IN nessie", random))
        .containsExactly(row("Branch", random, hash));

    commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = api.getReference().refName("main").get();

    assertThat(sql("ASSIGN BRANCH %s IN nessie", random))
        .containsExactly(row("Branch", random, main.getHash()));
  }

  @Test
  void testAssignTag() throws NessieConflictException, NessieNotFoundException {
    String random = "randomTag";
    assertThat(sql("CREATE TAG %s IN nessie", random)).containsExactly(row("Tag", random, hash));

    commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = api.getReference().refName("main").get();

    assertThat(sql("ASSIGN TAG %s IN nessie", random))
        .containsExactly(row("Tag", random, main.getHash()));
  }

  @Test
  void testAssignBranchTo() throws NessieConflictException, NessieNotFoundException {
    String random = "randomBranch";
    assertThat(sql("CREATE BRANCH %s IN nessie", random))
        .containsExactly(row("Branch", random, hash));

    commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = api.getReference().refName("main").get();

    assertThat(sql("ASSIGN BRANCH %s TO main IN nessie", random))
        .containsExactly(row("Branch", random, main.getHash()));

    for (Object[] commit : fetchLog("main")) {
      String currentHash = (String) commit[2];
      assertThat(sql("ASSIGN BRANCH %s TO main AT %s IN nessie", random, currentHash))
          .containsExactly(row("Branch", random, currentHash));
    }

    String invalidHash = "abc";
    String unknownHash = "dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9c";
    String invalidBranch = "invalidBranch";
    assertThatThrownBy(() -> sql("ASSIGN BRANCH %s TO main AT %s IN nessie", random, invalidHash))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(Validation.HASH_MESSAGE + " - but was: " + invalidHash);
    assertThatThrownBy(() -> sql("ASSIGN BRANCH %s TO main AT %s IN nessie", random, unknownHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Could not find commit '%s' in reference '%s'.", unknownHash, "main"));
    assertThatThrownBy(
            () -> sql("ASSIGN BRANCH %s TO %s AT %s IN nessie", random, invalidBranch, hash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Named reference '%s' not found", invalidBranch));
  }

  @Test
  void testAssignTagTo() throws NessieConflictException, NessieNotFoundException {
    String random = "randomTag";
    assertThat(sql("CREATE TAG %s IN nessie", random)).containsExactly(row("Tag", random, hash));

    commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = api.getReference().refName("main").get();

    assertThat(sql("ASSIGN TAG %s TO main IN nessie", random))
        .containsExactly(row("Tag", random, main.getHash()));

    List<Object[]> commits = fetchLog("main");
    for (Object[] commit : commits) {
      String currentHash = (String) commit[2];
      assertThat(sql("ASSIGN TAG %s TO main AT %s IN nessie", random, currentHash))
          .containsExactly(row("Tag", random, currentHash));
    }

    String invalidHash = "abc";
    String unknownHash = "dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9c";
    String invalidTag = "invalidTag";
    assertThatThrownBy(() -> sql("ASSIGN TAG %s TO main AT %s IN nessie", random, invalidHash))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(Validation.HASH_MESSAGE + " - but was: " + invalidHash);
    assertThatThrownBy(() -> sql("ASSIGN TAG %s TO main AT %s IN nessie", random, unknownHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Could not find commit '%s' in reference '%s'.", unknownHash, "main"));
    assertThatThrownBy(() -> sql("ASSIGN TAG %s TO %s AT %s IN nessie", random, invalidTag, hash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Named reference '%s' not found", invalidTag));
  }

  @Test
  void useShowReferencesIn() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("SHOW REFERENCE IN nessie")).containsExactly(row("Branch", refName, hash));
  }

  @Test
  void useShowReferencesAtTimestamp() throws NessieNotFoundException, NessieConflictException {
    commitAndReturnLog(refName);
    String time = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now(ZoneOffset.UTC));
    hash = api.getReference().refName(refName).get().getHash();
    assertThat(sql("USE REFERENCE %s AT `%s` IN nessie ", refName, time))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("SHOW REFERENCE IN nessie")).containsExactly(row("Branch", refName, hash));
  }

  @Test
  void useShowReferencesAtHash() throws NessieNotFoundException, NessieConflictException {
    List<Object[]> commits = commitAndReturnLog(refName);
    for (Object[] commit : commits) {
      String currentHash = (String) commit[2];
      assertThat(sql("USE REFERENCE %s AT %s IN nessie ", refName, currentHash))
          .containsExactly(row("Branch", refName, currentHash));
    }
  }

  @Test
  void useShowReferencesAtWithFailureConditions()
      throws NessieNotFoundException, NessieConflictException {
    commitAndReturnLog(refName);
    String randomHash = "dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9c";
    String invalidTimestamp = "01-01-01";
    String invalidBranch = "invalidBranch";
    String invalidHash = "abcdef123";
    assertThatThrownBy(() -> sql("USE REFERENCE %s AT %s IN nessie ", invalidBranch, hash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Named reference '%s' not found", invalidBranch));

    assertThatThrownBy(() -> sql("USE REFERENCE %s AT %s IN nessie ", refName, randomHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format("Could not find commit '%s' in reference '%s'.", randomHash, refName));

    assertThatThrownBy(() -> sql("USE REFERENCE %s AT `%s` IN nessie ", refName, invalidTimestamp))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageStartingWith(
            String.format(
                "Invalid timestamp provided: Text '%s' could not be parsed", invalidTimestamp));

    assertThatThrownBy(() -> sql("USE REFERENCE %s AT %s IN nessie ", refName, invalidHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageStartingWith(
            String.format(
                "Invalid timestamp provided: Text '%s' could not be parsed", invalidHash));
  }

  @Test
  void useShowReferences() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(api.getReference().refName(refName).get()).isEqualTo(Branch.of(refName, hash));

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("SHOW REFERENCE IN nessie")).containsExactly(row("Branch", refName, hash));
  }

  @Test
  void mergeReferencesIntoMain() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> commits =
        commitAndReturnLog(refName).stream()
            .map(AbstractSparkSqlTest::withoutHashAndTime)
            .collect(Collectors.toList());

    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    // here we are skipping commit time as its variable

    assertThat(
            sql("SHOW LOG main IN nessie").stream()
                .map(AbstractSparkSqlTest::sqlResultWithoutHashAndTime)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(commits);
  }

  @Test
  void mergeReferencesIn() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList =
        commitAndReturnLog(refName).stream()
            .map(AbstractSparkSqlTest::withoutHashAndTime)
            .collect(Collectors.toList());

    sql("MERGE BRANCH %s IN nessie", refName);
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG main IN nessie", refName).stream()
                .map(AbstractSparkSqlTest::sqlResultWithoutHashAndTime)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void mergeReferences() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList = commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH IN nessie");
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(AbstractSparkSqlTest::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);

    // omit the branch name to show log on main
    assertThat(
            sql("SHOW LOG IN nessie").stream()
                .map(AbstractSparkSqlTest::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void showLogIn() throws NessieConflictException, NessieNotFoundException, AnalysisException {
    List<Object[]> resultList = commitAndReturnLog(refName);
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(AbstractSparkSqlTest::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);

    // test to ensure property map is correctly encoded by Spark
    spark.sql(String.format("SHOW LOG %s IN nessie", refName)).createTempView("nessie_log");

    assertThat(
            spark
                .sql(
                    "SELECT author, committer, hash, message, signedOffBy, authorTime, committerTime, EXPLODE(properties) from nessie_log")
                .groupBy(
                    "author",
                    "committer",
                    "hash",
                    "message",
                    "signedOffBy",
                    "authorTime",
                    "committerTime")
                .pivot("key")
                .agg(functions.first("value"))
                .orderBy(functions.desc("committerTime"))
                .collectAsList()
                .stream()
                .map(AbstractSparkSqlTest::toJava)
                .map(AbstractSparkSqlTest::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(
            resultList.stream()
                .map(
                    x -> {
                      x[6] = ((Map<String, String>) x[6]).get("test");
                      return x;
                    })
                .collect(Collectors.toList()));
  }

  private List<Object[]> fetchLog(String branch) {
    return sql("SHOW LOG %s IN nessie", branch).stream()
        .map(AbstractSparkSqlTest::convert)
        .collect(Collectors.toList());
  }

  private List<Object[]> commitAndReturnLog(String branch)
      throws NessieConflictException, NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", branch))
        .containsExactly(row("Branch", branch, hash));
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
    Operations ops =
        ImmutableOperations.builder()
            .addOperations(Operation.Put.of(key, IcebergTable.of("foo", 42, 42, 42, 42)))
            .commitMeta(cm1)
            .build();
    Operations ops2 =
        ImmutableOperations.builder()
            .addOperations(Operation.Put.of(key, IcebergTable.of("bar", 42, 42, 42, 42)))
            .commitMeta(cm2)
            .build();
    Operations ops3 =
        ImmutableOperations.builder()
            .addOperations(Operation.Put.of(key, IcebergTable.of("baz", 42, 42, 42, 42)))
            .commitMeta(cm3)
            .build();

    Branch ref1 =
        api.commitMultipleOperations()
            .branchName(branch)
            .hash(hash)
            .operations(ops.getOperations())
            .commitMeta(ops.getCommitMeta())
            .commit();
    Branch ref2 =
        api.commitMultipleOperations()
            .branchName(branch)
            .hash(ref1.getHash())
            .operations(ops2.getOperations())
            .commitMeta(ops2.getCommitMeta())
            .commit();
    Branch ref3 =
        api.commitMultipleOperations()
            .branchName(branch)
            .hash(ref2.getHash())
            .operations(ops3.getOperations())
            .commitMeta(ops3.getCommitMeta())
            .commit();

    List<Object[]> resultList = new ArrayList<>();
    resultList.add(cmToRow(cm3, ref3.getHash()));
    resultList.add(cmToRow(cm2, ref2.getHash()));
    resultList.add(cmToRow(cm1, ref1.getHash()));
    return resultList;
  }

  @Test
  void showLog() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList = commitAndReturnLog(refName);

    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(AbstractSparkSqlTest::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void testInvalidCatalog() {
    assertThatThrownBy(() -> sql(String.format("LIST REFERENCES IN %s", NON_NESSIE_CATALOG)))
        .hasMessageContaining("The command works only when the catalog is a NessieCatalog")
        .hasMessageContaining(
            String.format("but %s is a org.apache.iceberg.hive.HiveCatalog", NON_NESSIE_CATALOG));

    // Catalog picked from the session
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    try {
      spark.sessionState().catalogManager().setCurrentCatalog(NON_NESSIE_CATALOG);
      assertThatThrownBy(() -> sql("LIST REFERENCES"))
          .hasMessageContaining("The command works only when the catalog is a NessieCatalog")
          .hasMessageContaining(
              String.format("but %s is a org.apache.iceberg.hive.HiveCatalog", NON_NESSIE_CATALOG));
    } finally {
      spark.sessionState().catalogManager().setCurrentCatalog(catalog);
    }
  }

  @Test
  void testValidCatalog() {
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Branch", "main", hash));

    // Catalog picked from the session
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    try {
      spark.sessionState().catalogManager().setCurrentCatalog("nessie");
      assertThat(sql("LIST REFERENCES")).containsExactlyInAnyOrder(row("Branch", "main", hash));
    } finally {
      spark.sessionState().catalogManager().setCurrentCatalog(catalog);
    }
  }

  private static Object[] convert(Object[] object) {
    return new Object[] {
      object[0], object[1], object[2], object[3], object[4], object[5], object[7]
    };
  }

  private static Object[] withoutHashAndTime(Object[] object) {
    return new Object[] {object[0], object[1], object[3], object[4], object[5], object[6]};
  }

  private static Object[] sqlResultWithoutHashAndTime(Object[] object) {
    return new Object[] {object[0], object[1], object[3], object[4], object[5], object[7]};
  }

  private Object[] cmToRow(CommitMeta cm, String hash) {
    return new Object[] {
      cm.getAuthor(),
      "",
      hash,
      cm.getMessage(),
      "",
      cm.getAuthorTime() == null ? null : Timestamp.from(cm.getAuthorTime()),
      cm.getProperties()
    };
  }
}
