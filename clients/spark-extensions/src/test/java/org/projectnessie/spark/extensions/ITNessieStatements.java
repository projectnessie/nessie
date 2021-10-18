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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.tests.AbstractSparkTest;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Validation;

public class ITNessieStatements extends AbstractSparkTest {

  private String hash;
  private final String refName = "testBranch";
  protected NessieClient nessieClient;

  @BeforeAll
  protected static void createDelta() {
    conf.set(
        "spark.sql.extensions", "org.projectnessie.spark.extensions.NessieSparkSessionExtensions");
  }

  @BeforeEach
  void getHash() throws NessieNotFoundException {
    nessieClient = NessieClient.builder().withUri(url).build();
    hash = nessieClient.getTreeApi().getDefaultBranch().getHash();
  }

  @AfterEach
  void removeBranches() throws NessieConflictException, NessieNotFoundException {
    for (Reference ref : nessieClient.getTreeApi().getAllReferences()) {
      if (ref instanceof Branch) {
        nessieClient.getTreeApi().deleteBranch(ref.getName(), ref.getHash());
      }
      if (ref instanceof Tag) {
        nessieClient.getTreeApi().deleteTag(ref.getName(), ref.getHash());
      }
    }
    nessieClient.getTreeApi().createReference(Branch.of("main", null));
  }

  @Test
  void testCreateBranchInExists() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));

    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    assertThat(sql("CREATE BRANCH IF NOT EXISTS %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));

    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    assertThatThrownBy(() -> sql("CREATE BRANCH %s IN nessie", refName))
        .isInstanceOf(NessieConflictException.class)
        .hasMessage("A reference of name [testBranch] already exists.");

    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
  }

  @Test
  public void testRefreshAfterMergeWithIcebergTableCaching() {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .hasSize(1)
        .containsExactly(row("Branch", refName, hash));

    sql("CREATE TABLE nessie.tbl (id int, name string)");
    sql("INSERT INTO nessie.tbl select 23, \"test\"");
    assertThat(sql("SELECT * FROM nessie.tbl")).hasSize(1).containsExactly(row(23, "test"));

    sql(String.format("MERGE BRANCH %s INTO main in nessie", refName));
    assertThat(sql("SELECT * FROM nessie.`tbl@main`")).hasSize(1).containsExactly(row(23, "test"));
    assertThat(sql("SELECT * FROM nessie.`tbl@%s`", refName))
        .hasSize(1)
        .containsExactly(row(23, "test"));
    sql("INSERT INTO nessie.tbl select 24, \"test24\"");
    assertThat(sql("SELECT * FROM nessie.`tbl@main`")).hasSize(1).containsExactly(row(23, "test"));
    assertThat(sql("SELECT * FROM nessie.tbl"))
        .hasSize(2)
        .containsExactlyInAnyOrder(row(23, "test"), row(24, "test24"));

    // this still sees old data because tbl@testBranch is being cached in Iceberg
    // due to "spark.sql.catalog.nessie.cache-enabled=true" by default
    assertThat(sql("SELECT * FROM nessie.`tbl@%s`", refName))
        .hasSize(1)
        .containsExactly(row(23, "test"));

    // using a fresh session with an empty cache sees the data correctly
    assertThat(sqlWithEmptyCache("SELECT * FROM nessie.`tbl@%s`", refName))
        .hasSize(2)
        .containsExactlyInAnyOrder(row(23, "test"), row(24, "test24"));
  }

  private static List<Object[]> sqlWithEmptyCache(String query, Object... args) {
    try (SparkSession sparkWithEmptyCache = spark.cloneSession()) {
      List<Row> rows = sparkWithEmptyCache.sql(String.format(query, args)).collectAsList();
      return rows.stream().map(AbstractSparkTest::toJava).collect(Collectors.toList());
    }
  }

  @Test
  void testCreateBranchIn() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));

    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
  }

  @Test
  void testCreateTagIn() throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie", refName)).containsExactly(row("Tag", refName, hash));
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Tag.of(refName, hash));
    assertThat(sql("DROP TAG %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void testCreateBranchInFrom() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie FROM main", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void testCreateTagInFrom() throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie FROM main", refName))
        .containsExactly(row("Tag", refName, hash));
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Tag.of(refName, hash));
    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Branch", "main", hash), row("Tag", refName, hash));
    assertThat(sql("DROP TAG %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void testAssignBranch() throws NessieConflictException, NessieNotFoundException {
    String random = "randomBranch";
    assertThat(sql("CREATE BRANCH %s IN nessie", random))
        .containsExactly(row("Branch", random, hash));

    commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = nessieClient.getTreeApi().getReferenceByName("main");

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
    Reference main = nessieClient.getTreeApi().getReferenceByName("main");

    assertThat(sql("ASSIGN TAG %s IN nessie", random))
        .containsExactly(row("Tag", random, main.getHash()));
  }

  @Test
  void testAssignBranchTo() throws NessieConflictException, NessieNotFoundException {
    String random = "randomBranch";
    assertThat(sql("CREATE BRANCH %s IN nessie", random))
        .containsExactly(row("Branch", random, hash));

    List<Object[]> commits = commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = nessieClient.getTreeApi().getReferenceByName("main");

    assertThat(sql("ASSIGN BRANCH %s TO main IN nessie", random))
        .containsExactly(row("Branch", random, main.getHash()));

    for (Object[] commit : commits) {
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
        .hasMessage("Unable to find a ref or hash provided.");
    assertThatThrownBy(
            () -> sql("ASSIGN BRANCH %s TO %s AT %s IN nessie", random, invalidBranch, hash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Unable to find reference [%s].", invalidBranch));
  }

  @Test
  void testAssignTagTo() throws NessieConflictException, NessieNotFoundException {
    String random = "randomTag";
    assertThat(sql("CREATE TAG %s IN nessie", random)).containsExactly(row("Tag", random, hash));

    List<Object[]> commits = commitAndReturnLog(refName);
    sql("USE REFERENCE %s IN nessie", refName);
    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    Reference main = nessieClient.getTreeApi().getReferenceByName("main");

    assertThat(sql("ASSIGN TAG %s TO main IN nessie", random))
        .containsExactly(row("Tag", random, main.getHash()));

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
        .hasMessage("Unable to find a ref or hash provided.");
    assertThatThrownBy(() -> sql("ASSIGN TAG %s TO %s AT %s IN nessie", random, invalidTag, hash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage(String.format("Unable to find reference [%s].", invalidTag));
  }

  @Test
  void testCreateBranch() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Branch", refName, hash), row("Branch", "main", hash));
    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void testCreateTag() throws NessieNotFoundException {
    assertThat(sql("CREATE TAG %s IN nessie", refName)).containsExactly(row("Tag", refName, hash));
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Tag.of(refName, hash));
    // Result of LIST REFERENCES does not guarantee any order
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Tag", refName, hash), row("Branch", "main", hash));
    assertThat(sql("DROP TAG %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void useShowReferencesIn() throws NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("SHOW REFERENCE IN nessie")).containsExactly(row("Branch", refName, hash));

    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void useShowReferencesAtTimestamp() throws NessieNotFoundException, NessieConflictException {
    commitAndReturnLog(refName);
    String time = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now(ZoneOffset.UTC));
    hash = nessieClient.getTreeApi().getReferenceByName(refName).getHash();
    assertThat(sql("USE REFERENCE %s AT `%s` IN nessie ", refName, time))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("SHOW REFERENCE IN nessie")).containsExactly(row("Branch", refName, hash));

    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void useShowReferencesAtHash() throws NessieNotFoundException, NessieConflictException {
    for (Object[] commit : commitAndReturnLog(refName)) {
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
        .hasMessage(
            String.format("Could not find commit '%s' in reference '%s'.", hash, invalidBranch));

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
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    assertThat(sql("USE REFERENCE %s IN nessie", refName))
        .containsExactly(row("Branch", refName, hash));
    assertThat(sql("SHOW REFERENCE IN nessie")).containsExactly(row("Branch", refName, hash));

    assertThat(sql("DROP BRANCH %s IN nessie", refName)).containsExactly(row("OK"));
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void mergeReferencesIntoMain() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList = commitAndReturnLog(refName);

    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG main IN nessie", refName).stream()
                .map(ITNessieStatements::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void mergeReferencesIn() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList = commitAndReturnLog(refName);

    sql("MERGE BRANCH %s IN nessie", refName);
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG main IN nessie", refName).stream()
                .map(ITNessieStatements::convert)
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
                .map(ITNessieStatements::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);

    // omit the branch name to show log on main
    assertThat(
            sql("SHOW LOG IN nessie").stream()
                .map(ITNessieStatements::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void showLogIn() throws NessieConflictException, NessieNotFoundException, AnalysisException {
    List<Object[]> resultList = commitAndReturnLog(refName);
    // here we are skipping commit time as its variable
    assertThat(
            sql("SHOW LOG %s IN nessie", refName).stream()
                .map(ITNessieStatements::convert)
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
                .map(AbstractSparkTest::toJava)
                .map(ITNessieStatements::convert)
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

  private List<Object[]> commitAndReturnLog(String branch)
      throws NessieConflictException, NessieNotFoundException {
    assertThat(sql("CREATE BRANCH %s IN nessie", branch))
        .containsExactly(row("Branch", branch, hash));
    ContentsKey key = ContentsKey.of("table", "name");
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
            .addOperations(Operation.Put.of(key, IcebergTable.of("foo")))
            .commitMeta(cm1)
            .build();
    Operations ops2 =
        ImmutableOperations.builder()
            .addOperations(Operation.Put.of(key, IcebergTable.of("bar")))
            .commitMeta(cm2)
            .build();
    Operations ops3 =
        ImmutableOperations.builder()
            .addOperations(Operation.Put.of(key, IcebergTable.of("baz")))
            .commitMeta(cm3)
            .build();

    Branch ref1 = nessieClient.getTreeApi().commitMultipleOperations(branch, hash, ops);
    Branch ref2 = nessieClient.getTreeApi().commitMultipleOperations(branch, ref1.getHash(), ops2);
    Branch ref3 = nessieClient.getTreeApi().commitMultipleOperations(branch, ref2.getHash(), ops3);

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
                .map(ITNessieStatements::convert)
                .collect(Collectors.toList()))
        .containsExactlyElementsOf(resultList);
  }

  @Test
  void testInvalidCatalog() {
    assertThatThrownBy(() -> sql("LIST REFERENCES IN hive"))
        .hasMessage(
            "requirement failed: The command works only when the catalog is a NessieCatalog. Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>.");

    // Catalog picked from the session
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("hive");
    assertThatThrownBy(() -> sql("LIST REFERENCES"))
        .hasMessage(
            "requirement failed: The command works only when the catalog is a NessieCatalog. Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>.");
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);
  }

  @Test
  void testValidCatalog() {
    assertThat(sql("LIST REFERENCES IN nessie"))
        .containsExactlyInAnyOrder(row("Branch", "main", hash));

    // Catalog picked from the session
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    assertThat(sql("LIST REFERENCES")).containsExactlyInAnyOrder(row("Branch", "main", hash));
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);
  }

  private static Object[] convert(Object[] object) {
    return new Object[] {
      object[0], object[1], object[2], object[3], object[4], object[5], object[7]
    };
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
