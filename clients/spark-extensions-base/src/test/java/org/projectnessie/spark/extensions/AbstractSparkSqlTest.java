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
import com.google.errorprone.annotations.FormatMethod;
import java.io.File;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  private Branch initialDefaultBranch;

  private String refName;
  private String additionalRefName;
  protected NessieApiV1 api;

  @BeforeEach
  void setupSparkAndApi(TestInfo testInfo) throws NessieNotFoundException {
    api = HttpClientBuilder.builder().withUri(url).build(NessieApiV1.class);

    refName = testInfo.getTestMethod().map(Method::getName).get();
    additionalRefName = refName + "_other";

    initialDefaultBranch = api.getDefaultBranch();

    Map<String, String> nessieParams =
        ImmutableMap.of(
            "ref", defaultBranch(), "uri", url, "warehouse", tempFile.toURI().toString());

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
  }

  @AfterEach
  void removeBranches() throws NessieConflictException, NessieNotFoundException {
    // Reset potential "USE REFERENCE" statements from previous tests
    SparkSession.active()
        .sparkContext()
        .conf()
        .set(String.format("spark.sql.catalog.%s.ref", "nessie"), defaultBranch())
        .remove(String.format("spark.sql.catalog.%s.ref.hash", "nessie"));

    Branch defaultBranch = api.getDefaultBranch();
    for (Reference ref : api.getAllReferences().get().getReferences()) {
      if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
        api.deleteBranch().branchName(ref.getName()).hash(ref.getHash()).delete();
      }
      if (ref instanceof Tag) {
        api.deleteTag().tagName(ref.getName()).hash(ref.getHash()).delete();
      }
    }
    api.assignBranch().assignTo(initialDefaultBranch).branch(defaultBranch).assign();
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

  @Test
  public void testRefreshAfterMergeWithIcebergTableCaching() throws NessieNotFoundException {
    createBranchForTest(refName);
    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .hasSize(1)
        .containsExactly(row("Branch", refName, defaultHash()));

    sql("CREATE TABLE nessie.db.tbl (id int, name string)");
    sql("INSERT INTO nessie.db.tbl select 23, \"test\"");
    assertThat(sql("SELECT * FROM nessie.db.tbl")).hasSize(1).containsExactly(row(23, "test"));

    sql("MERGE BRANCH %s INTO %s in nessie", refName, defaultBranch());
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

  @Test
  void testCreateBranchInExists() throws NessieNotFoundException {
    createBranchForTest(refName);

    assertThat(sql("CREATE BRANCH IF NOT EXISTS %s IN nessie", refName))
        .containsExactly(row("Branch", refName, defaultHash()));
    assertThat(api.getReference().refName(refName).get())
        .isEqualTo(Branch.of(refName, defaultHash()));

    assertThatThrownBy(() -> sql("CREATE BRANCH %s IN nessie", refName))
        .isInstanceOf(NessieConflictException.class)
        .hasMessage("Named reference '%s' already exists.", refName);
  }

  @Test
  void testCreateBranch() throws NessieNotFoundException {
    createBranchForTest(refName);

    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(
            row("Branch", refName, defaultHash()), row("Branch", defaultBranch(), defaultHash()));
  }

  @Test
  void testCreateTag() throws NessieNotFoundException {
    createTagForTest(refName);

    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(
            row("Tag", refName, defaultHash()), row("Branch", defaultBranch(), defaultHash()));
  }

  @Test
  void testCreateReferenceFromHashOnNonDefaultBranch() throws NessieNotFoundException {
    createBranchForTest(refName);

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
    createBranchForTest(refName);

    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> api.getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference '%s' not found", refName);
  }

  @Test
  void testDropTagIn() throws NessieNotFoundException {
    createTagForTest(refName);

    assertThat(sql("DROP TAG %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> api.getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference '%s' not found", refName);
  }

  @Test
  void testAssignBranch() throws NessieConflictException, NessieNotFoundException {
    createBranchForTest(additionalRefName);

    createBranchCommitAndReturnLog();
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO %s IN nessie", refName, defaultBranch());
    Reference main = api.getReference().refName(defaultBranch()).get();

    assertThat(sql("ASSIGN BRANCH %s TO %s IN nessie", additionalRefName, defaultBranch()))
        .containsExactly(row("Branch", additionalRefName, main.getHash()));
  }

  @Test
  void testAssignTag() throws NessieConflictException, NessieNotFoundException {
    createTagForTest(additionalRefName);

    createBranchCommitAndReturnLog();
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO %s IN nessie", refName, defaultBranch());
    Reference main = api.getReference().refName(defaultBranch()).get();

    assertThat(sql("ASSIGN TAG %s TO %s IN nessie", additionalRefName, defaultBranch()))
        .containsExactly(row("Tag", additionalRefName, main.getHash()));
  }

  @Test
  void testAssignBranchTo() throws NessieConflictException, NessieNotFoundException {
    createBranchForTest(additionalRefName);

    createBranchCommitAndReturnLog();
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO %s IN nessie", refName, defaultBranch());
    Reference main = api.getReference().refName(defaultBranch()).get();

    assertThat(sql("ASSIGN BRANCH %s TO %s IN nessie", additionalRefName, defaultBranch()))
        .containsExactly(row("Branch", additionalRefName, main.getHash()));

    for (SparkCommitLogEntry commit : fetchLog(defaultBranch())) {
      String currentHash = commit.getHash();
      assertThat(
              sql(
                  "ASSIGN BRANCH %s TO %s AT %s IN nessie",
                  additionalRefName, defaultBranch(), currentHash))
          .containsExactly(row("Branch", additionalRefName, currentHash));
    }

    String invalidHash = "abc";
    String unknownHash = "dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9c";
    String invalidBranch = "invalidBranch";
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN BRANCH %s TO %s AT %s IN nessie",
                    additionalRefName, defaultBranch(), invalidHash))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(Validation.HASH_MESSAGE + " - but was: " + invalidHash);
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN BRANCH %s TO %s AT %s IN nessie",
                    additionalRefName, defaultBranch(), unknownHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format(
                "Could not find commit '%s' in reference '%s'.", unknownHash, defaultBranch()));
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN BRANCH %s TO %s AT %s IN nessie",
                    additionalRefName, invalidBranch, defaultHash()))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Named reference '%s' not found", invalidBranch));
  }

  @Test
  void testAssignTagTo() throws NessieConflictException, NessieNotFoundException {
    createTagForTest(additionalRefName);

    createBranchCommitAndReturnLog();
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO %s IN nessie", refName, defaultBranch());
    Reference main = api.getReference().refName(defaultBranch()).get();

    assertThat(sql("ASSIGN TAG %s TO %s IN nessie", additionalRefName, defaultBranch()))
        .containsExactly(row("Tag", additionalRefName, main.getHash()));

    List<SparkCommitLogEntry> commits = fetchLog(defaultBranch());
    for (SparkCommitLogEntry commit : commits) {
      String currentHash = commit.getHash();
      assertThat(
              sql(
                  "ASSIGN TAG %s TO %s AT %s IN nessie",
                  additionalRefName, defaultBranch(), currentHash))
          .containsExactly(row("Tag", additionalRefName, currentHash));
    }

    String invalidHash = "abc";
    String unknownHash = "dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9c";
    String invalidTag = "invalidTag";
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN TAG %s TO %s AT %s IN nessie",
                    additionalRefName, defaultBranch(), invalidHash))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(Validation.HASH_MESSAGE + " - but was: " + invalidHash);
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN TAG %s TO %s AT %s IN nessie",
                    additionalRefName, defaultBranch(), unknownHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(
            String.format(
                "Could not find commit '%s' in reference '%s'.", unknownHash, defaultBranch()));
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN TAG %s TO %s AT %s IN nessie",
                    additionalRefName, invalidTag, defaultHash()))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Named reference '%s' not found", invalidTag));
  }

  @Test
  void useShowReferencesIn() throws NessieNotFoundException {
    createBranchForTest(refName);

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .containsExactly(row("Branch", refName, defaultHash()));
    assertThat(sql("SHOW REFERENCE IN nessie"))
        .containsExactly(row("Branch", refName, defaultHash()));
  }

  @Test
  void throwWhenUseShowReferencesAtTimestampWithoutTimeZone()
      throws NessieNotFoundException, NessieConflictException {
    createBranchCommitAndReturnLog();
    String time = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now(ZoneOffset.UTC));
    assertThatThrownBy(() -> sql("USE REFERENCE %s AT `%s` IN nessie ", refName, time))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format("Invalid timestamp provided: Text '%s' could not be parsed ", time))
        .hasMessageContaining(
            "You need to provide it with a zone info. For more info, see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
  }

  @Test
  void throwWhenUseShowReferencesAtTimestampBeforeCommits()
      throws NessieNotFoundException, NessieConflictException {
    createBranchCommitAndReturnLog();

    // get the last commitTime
    Instant commitTime =
        api.getCommitLog()
            .refName(refName)
            .get()
            .getLogEntries()
            .get(0)
            .getCommitMeta()
            .getCommitTime();
    assertThat(commitTime).isNotNull();
    // query for commits that were executed at least one hour before - there will be no commit for
    // this predicate
    String timeWithZone =
        DateTimeFormatter.ISO_INSTANT
            .withZone(ZoneId.of("UTC"))
            .format(commitTime.minus(1, ChronoUnit.HOURS));

    assertThatThrownBy(() -> sql("USE REFERENCE %s AT `%s` IN nessie ", refName, timeWithZone))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(String.format("Cannot find a hash before %s.", timeWithZone));
  }

  @ParameterizedTest
  @MethodSource("dateTimeFormatProvider")
  void useShowReferencesAtTimestampWithTimeZone(DateTimeFormatter dateTimeFormatter)
      throws NessieNotFoundException, NessieConflictException {
    List<SparkCommitLogEntry> commits = createBranchCommitAndReturnLog();

    // get the last commitTime
    Instant commitTime =
        api.getCommitLog()
            .refName(refName)
            .get()
            .getLogEntries()
            .get(0)
            .getCommitMeta()
            .getCommitTime();
    assertThat(commitTime).isNotNull();
    String timeWithZone = dateTimeFormatter.format(commitTime);

    // the last commit is on the index 0
    SparkCommitLogEntry lastCommitBeforeTimePredicate = commits.get(0);
    commitAndReturnLog(refName, lastCommitBeforeTimePredicate.getHash());

    String lastRefHashGlobally = api.getReference().refName(refName).get().getHash();
    // it should not include the current last hash
    // api.getReference().refName(refName).get().getHash()
    // because the last hash was committed after the commitTime
    assertThat(sql("USE REFERENCE %s AT `%s` IN nessie ", refName, timeWithZone))
        .containsExactly(row("Branch", refName, lastCommitBeforeTimePredicate.getHash()));
    assertThat(sql("SHOW REFERENCE IN nessie"))
        .containsExactly(row("Branch", refName, lastRefHashGlobally));
  }

  private static Stream<Arguments> dateTimeFormatProvider() {
    // it can be any time zone
    ZoneId localZone = ZoneId.of("+02");
    return Stream.of(
        Arguments.of(DateTimeFormatter.ISO_ZONED_DATE_TIME.withZone(localZone)),
        Arguments.of(DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(localZone)),
        Arguments.of(DateTimeFormatter.ISO_INSTANT.withZone(localZone)),
        Arguments.of(DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"))));
  }

  @Test
  void useShowReferencesAtHash() throws NessieNotFoundException, NessieConflictException {
    List<SparkCommitLogEntry> commits = createBranchCommitAndReturnLog();
    for (SparkCommitLogEntry commit : commits) {
      String currentHash = commit.getHash();
      assertThat(sql("USE REFERENCE %s AT %s IN nessie ", refName, currentHash))
          .containsExactly(row("Branch", refName, currentHash));
    }
  }

  @Test
  void useShowReferencesAtWithFailureConditions()
      throws NessieNotFoundException, NessieConflictException {
    createBranchCommitAndReturnLog();
    String randomHash = "dd8d46a3dd5478ce69749a5455dba29d74f6d1171188f4c21d0e15ff4a0a9a9c";
    String invalidTimestamp = "01-01-01";
    String invalidBranch = "invalidBranch";
    String invalidHash = "abcdef1234";
    assertThatThrownBy(() -> sql("USE REFERENCE %s AT %s IN nessie ", invalidBranch, defaultHash()))
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
            String.format("Could not find commit '%s' in reference '%s'", invalidHash, refName));
  }

  @Test
  void useShowReferences() throws NessieNotFoundException {
    createBranchForTest(refName);

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .containsExactly(row("Branch", refName, defaultHash()));
    assertThat(sql("SHOW REFERENCE IN nessie"))
        .containsExactly(row("Branch", refName, defaultHash()));
  }

  @Test
  void mergeReferencesIn() throws NessieConflictException, NessieNotFoundException {
    mergeReferencesInto("MERGE BRANCH %s IN nessie");
  }

  @Test
  void mergeReferencesIntoMain() throws NessieConflictException, NessieNotFoundException {
    mergeReferencesInto("MERGE BRANCH %s INTO " + defaultBranch() + " IN nessie");
  }

  @SuppressWarnings("FormatStringAnnotation")
  private void mergeReferencesInto(String query)
      throws NessieConflictException, NessieNotFoundException {
    SparkCommitLogEntry result =
        createBranchCommitAndReturnLog().stream()
            .map(SparkCommitLogEntry::withoutHashAndTime)
            .reduce(SparkCommitLogEntry::mergedCommits)
            .map(SparkCommitLogEntry::relevantFromMerge)
            .get();

    sql(query, refName);
    // here we are skipping commit time as its variable

    assertThat(
            sql("SHOW LOG %s IN nessie", defaultBranch()).stream()
                .map(SparkCommitLogEntry::fromShowLog)
                .map(SparkCommitLogEntry::withoutHashAndTime)
                .map(SparkCommitLogEntry::relevantFromMerge)
                .collect(Collectors.toList()))
        .containsExactly(result);
  }

  @Test
  void mergeReferences() throws NessieConflictException, NessieNotFoundException {
    List<SparkCommitLogEntry> resultList = createBranchCommitAndReturnLog();
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH IN nessie");
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(SparkCommitLogEntry::fromShowLog)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);

    // omit the branch name to show log on main
    assertThat(
            sql("SHOW LOG IN nessie").stream()
                .map(SparkCommitLogEntry::fromShowLog)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void showLogIn() throws NessieConflictException, NessieNotFoundException, AnalysisException {
    List<SparkCommitLogEntry> resultList = createBranchCommitAndReturnLog();
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(SparkCommitLogEntry::fromShowLog)
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
                .peek(row -> row[7] = Collections.singletonMap("test", (String) row[7]))
                .map(SparkCommitLogEntry::fromShowLog)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void showLog() throws NessieConflictException, NessieNotFoundException {
    List<SparkCommitLogEntry> resultList = createBranchCommitAndReturnLog();

    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(SparkCommitLogEntry::fromShowLog)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void testInvalidCatalog() {
    assertThatThrownBy(() -> sql("LIST REFERENCES IN %s", NON_NESSIE_CATALOG))
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
        .containsExactlyInAnyOrder(row("Branch", defaultBranch(), defaultHash()));

    // Catalog picked from the session
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    try {
      spark.sessionState().catalogManager().setCurrentCatalog("nessie");
      assertThat(sql("LIST REFERENCES"))
          .containsExactlyInAnyOrder(row("Branch", defaultBranch(), defaultHash()));
    } finally {
      spark.sessionState().catalogManager().setCurrentCatalog(catalog);
    }
  }

  private String defaultBranch() {
    return initialDefaultBranch.getName();
  }

  private String defaultHash() {
    return initialDefaultBranch.getHash();
  }

  @FormatMethod
  protected static List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rows.stream().map(AbstractSparkSqlTest::toJava).collect(Collectors.toList());
  }

  @FormatMethod
  private static List<Object[]> sqlWithEmptyCache(String query, Object... args) {
    try (SparkSession sparkWithEmptyCache = spark.cloneSession()) {
      List<Row> rows = sparkWithEmptyCache.sql(String.format(query, args)).collectAsList();
      return rows.stream().map(AbstractSparkSqlTest::toJava).collect(Collectors.toList());
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

  private List<SparkCommitLogEntry> fetchLog(String branch) {
    return sql("SHOW LOG %s IN nessie", branch).stream()
        .map(SparkCommitLogEntry::fromShowLog)
        .collect(Collectors.toList());
  }

  private void createBranchForTest(String branchName) throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", branchName))
        .containsExactly(row("Branch", branchName, defaultHash()));
    assertThat(api.getReference().refName(branchName).get())
        .isEqualTo(Branch.of(branchName, defaultHash()));
  }

  private void createTagForTest(String tagName) throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie", tagName))
        .containsExactly(row("Tag", tagName, defaultHash()));
    assertThat(api.getReference().refName(tagName).get()).isEqualTo(Tag.of(tagName, defaultHash()));
  }

  private List<SparkCommitLogEntry> createBranchCommitAndReturnLog()
      throws NessieConflictException, NessieNotFoundException {
    createBranchForTest(refName);
    return commitAndReturnLog(refName, defaultHash());
  }

  private List<SparkCommitLogEntry> commitAndReturnLog(String branch, String initalHashOrBranch)
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
            .hash(initalHashOrBranch)
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

    List<SparkCommitLogEntry> resultList = new ArrayList<>();
    resultList.add(SparkCommitLogEntry.fromCommitMeta(cm3, ref3.getHash()));
    resultList.add(SparkCommitLogEntry.fromCommitMeta(cm2, ref2.getHash()));
    resultList.add(SparkCommitLogEntry.fromCommitMeta(cm1, ref1.getHash()));
    return resultList;
  }
}
