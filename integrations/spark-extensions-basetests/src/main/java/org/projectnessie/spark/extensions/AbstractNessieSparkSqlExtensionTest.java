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

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

public abstract class AbstractNessieSparkSqlExtensionTest extends SparkSqlTestBase {

  @TempDir File tempFile;

  @Override
  protected boolean requiresCommonAncestor() {
    return true;
  }

  @Override
  protected String warehouseURI() {
    return tempFile.toURI().toString();
  }

  @BeforeAll
  protected static void useNessieExtensions() {
    conf.set(
        "spark.sql.extensions",
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            + ","
            + "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
  }

  @ValueSource(strings = {"backquoted", "back/quoted"})
  @ParameterizedTest
  public void backquotedRefName(String branchName) throws NessieNotFoundException {
    assertThat(
            sql("CREATE BRANCH `%s` IN nessie FROM %s", branchName, initialDefaultBranch.getName()))
        .containsExactly(row("Branch", branchName, defaultHash()));
    Reference ref = api.getReference().refName(branchName).get();

    assertThat(sql("USE REFERENCE `%s` IN nessie", branchName))
        .hasSize(1)
        .containsExactly(row("Branch", branchName, ref.getHash()));

    assertThat(sql("SHOW LOG `%s` IN nessie", branchName)).isNotEmpty();

    assertThat(sql("DROP BRANCH `%s` IN nessie", branchName)).hasSize(1).containsExactly(row("OK"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "USE REFERENCE %s IN nessie",
        "/* leading */ CREATE BRANCH IF NOT EXISTS %s_other IN nessie",
        "/* some comment here */ USE REFERENCE %s IN nessie /* and there */ ",
        "/* some comment here */ USE REFERENCE %s IN nessie -- and there",
        "/* some comment here */\nUSE REFERENCE %s IN nessie\n-- and there",
        "/* \nsome \ncomment \nhere */\nUSE REFERENCE %s IN nessie\n-- and there",
        "/* leading -- leading \n */ CREATE BRANCH IF NOT EXISTS %s_other IN nessie",
        "-- leading \n CREATE BRANCH IF NOT EXISTS %s_other IN nessie",
        " -- leading \n -- leading \n -- leading \n CREATE BRANCH IF NOT EXISTS %s_other IN nessie",
      })
  @SuppressWarnings("FormatStringAnnotation")
  public void testComments(String sql) throws NessieNotFoundException {
    createBranchForTest(refName);
    sql(sql, refName);
  }

  @Test
  public void testRefreshAfterMergeWithIcebergTableCaching()
      throws NessieNotFoundException, NessieNamespaceAlreadyExistsException {
    createBranchForTest(refName);
    api.createNamespace().refName(refName).namespace(Namespace.of("db")).create();
    Reference ref = api.getReference().refName(refName).get();

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .hasSize(1)
        .containsExactly(row("Branch", refName, ref.getHash()));

    sql("CREATE TABLE nessie.db.tbl (id int, name string)");
    sql("INSERT INTO nessie.db.tbl select 23, \"test\"");
    assertThat(sql("SELECT * FROM nessie.db.tbl")).hasSize(1).containsExactly(row(23, "test"));

    sql("MERGE BRANCH `%s` INTO `%s` in nessie", refName, defaultBranch());
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
  void testCreateBranchIfNotExists() throws NessieNotFoundException {
    createBranchForTest(refName);

    assertThat(sql("CREATE BRANCH IF NOT EXISTS %s IN nessie", refName))
        .containsExactly(row("Branch", refName, defaultHash()));
    assertThat(api.getReference().refName(refName).get())
        .isEqualTo(Branch.of(refName, defaultHash()));

    assertThat(sql("CREATE BRANCH IF NOT EXISTS %s IN nessie", refName))
        .containsExactly(row("Branch", refName, defaultHash()));
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
  void testCreateReferenceFromHashOnNonDefaultBranch()
      throws NessieNotFoundException, NessieNamespaceAlreadyExistsException {
    createBranchForTest(refName);
    api.createNamespace().refName(refName).namespace(Namespace.of("db")).create();

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
  void testDropBranchIfExists() throws NessieNotFoundException {
    createBranchForTest(refName);

    assertThat(sql("DROP BRANCH IF EXISTS %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> api.getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference '%s' not found", refName);
    assertThat(sql("DROP BRANCH IF EXISTS %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> sql("DROP BRANCH %s IN nessie", refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference '%s' not found", refName);
  }

  @Test
  void testDropTagIfExists() throws NessieNotFoundException {
    createTagForTest(refName);

    assertThat(sql("DROP TAG IF EXISTS %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> api.getReference().refName(refName).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Named reference '%s' not found", refName);
    assertThat(sql("DROP TAG IF EXISTS %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> sql("DROP TAG %s IN nessie", refName))
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
        .hasMessage(Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + invalidHash);
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN BRANCH %s TO %s AT %s IN nessie",
                    additionalRefName, defaultBranch(), unknownHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Commit '%s' not found", unknownHash));
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
        .hasMessage(Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + invalidHash);
    assertThatThrownBy(
            () ->
                sql(
                    "ASSIGN TAG %s TO %s AT %s IN nessie",
                    additionalRefName, defaultBranch(), unknownHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Commit '%s' not found", unknownHash));
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

    // it should not include the current last hash
    // api.getReference().refName(refName).get().getHash()
    // because the last hash was committed after the commitTime
    assertThat(sql("USE REFERENCE %s AT `%s` IN nessie ", refName, timeWithZone))
        .containsExactly(row("Branch", refName, lastCommitBeforeTimePredicate.getHash()));
    assertThat(sql("SHOW REFERENCE IN nessie"))
        .containsExactly(row("Branch", refName, lastCommitBeforeTimePredicate.getHash()));
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
      assertThat(sql("SHOW REFERENCE IN nessie"))
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
        .hasMessage(String.format("Commit '%s' not found", randomHash));

    assertThatThrownBy(() -> sql("USE REFERENCE %s AT `%s` IN nessie ", refName, invalidTimestamp))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageStartingWith(
            String.format(
                "Invalid timestamp provided: Text '%s' could not be parsed", invalidTimestamp));

    assertThatThrownBy(() -> sql("USE REFERENCE %s AT %s IN nessie ", refName, invalidHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Commit '%s' not found", invalidHash));
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

    result = ImmutableSparkCommitLogEntry.builder().from(result).properties(emptyMap()).build();

    sql(query, refName);
    // here we are skipping commit time as its variable

    assertThat(
            sql("SHOW LOG %s IN nessie", defaultBranch()).stream()
                .map(SparkCommitLogEntry::fromShowLog)
                .filter(e -> !e.getMessage().startsWith("INFRA: "))
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
                .filter(e -> !e.getMessage().startsWith("INFRA: "))
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);

    // omit the branch name to show log on main
    assertThat(
            sql("SHOW LOG IN nessie").stream()
                .map(SparkCommitLogEntry::fromShowLog)
                .filter(e -> !e.getMessage().startsWith("INFRA: "))
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
                .filter(e -> !e.getMessage().startsWith("INFRA: "))
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
                .map(AbstractNessieSparkSqlExtensionTest::toJava)
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
                .filter(e -> !e.getMessage().startsWith("INFRA: "))
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

  @ParameterizedTest
  @CsvSource({
    "testCompaction,tbl",
    "main,tbl",
    "testCompaction,`tbl@testCompaction`",
    "main,`tbl@main`"
  })
  void testCompaction(String branchName, String tableName)
      throws NessieNotFoundException, NessieNamespaceAlreadyExistsException {
    executeAndValidateCompaction(branchName, prepareForCompaction(branchName), tableName);
  }

  String prepareForCompaction(String branchName)
      throws NessieNotFoundException, NessieNamespaceAlreadyExistsException {
    if (!branchName.equals("main")) {
      assertThat(sql("CREATE BRANCH %s IN nessie FROM main", branchName))
          .containsExactly(row("Branch", branchName, initialDefaultBranch.getHash()));
    }

    api.createNamespace().refName(branchName).namespace(Namespace.of("db")).create();

    sql("USE REFERENCE %s IN nessie", branchName);
    sql("CREATE TABLE nessie.db.tbl (id int, name string)");
    sql("INSERT INTO nessie.db.tbl select 23, \"test\"");
    sql("INSERT INTO nessie.db.tbl select 24, \"test24\"");

    String branchHash = api.getReference().refName(branchName).get().getHash();
    assertThat(sql("CREATE BRANCH dev IN nessie FROM %s", branchName))
        .containsExactly(row("Branch", "dev", branchHash));
    return branchHash;
  }

  void executeAndValidateCompaction(String branchName, String branchHash, String tableName)
      throws NessieNotFoundException {
    List<Object[]> result = executeCompaction(tableName);
    // re-written files count is 2 and the added files count is 1
    assertThat(result.get(0)[0]).isEqualTo(2);
    assertThat(result.get(0)[1]).isEqualTo(1);

    // check for compaction commit
    LogResponse.LogEntry logEntry =
        api.getCommitLog().refName(branchName).maxRecords(1).get().getLogEntries().get(0);
    assertThat(logEntry.getCommitMeta().getMessage())
        .isIn(
            // Non-RESTCatalog
            "Iceberg replace against db.tbl",
            // RESTCatalog
            "Update ICEBERG_TABLE db.tbl");

    assertThat(sql("SELECT * FROM nessie.db.tbl"))
        .hasSize(2)
        .containsExactlyInAnyOrder(row(23, "test"), row(24, "test24"));

    // same table in other branch should not be modified
    assertThat(api.getReference().refName("dev").get().getHash()).isEqualTo(branchHash);
  }

  static List<Object[]> executeCompaction(String tableName) {
    return sql(
        "CALL nessie.system.rewrite_data_files(table => 'nessie.db.%s', options => map"
            + "('min-input-files','2'))",
        tableName);
  }
}
