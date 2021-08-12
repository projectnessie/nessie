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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.tests.AbstractSparkTest;
import org.projectnessie.error.BaseNessieClientServerException;
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
import org.projectnessie.model.Tag;

public class ITNessieStatements extends AbstractSparkTest {

  private String hash;
  private final String refName = "testBranch";

  @BeforeAll
  protected static void createDelta() {
    conf.set(
        "spark.sql.extensions", "org.projectnessie.spark.extensions.NessieSparkSessionExtensions");
  }

  @BeforeEach
  void getHash() throws NessieNotFoundException {
    hash = nessieClient.getTreeApi().getDefaultBranch().getHash();
  }

  @AfterEach
  void removeBranches() throws NessieConflictException, NessieNotFoundException {
    for (String s : Arrays.asList(refName, "main")) {
      try {
        nessieClient
            .getTreeApi()
            .deleteBranch(s, nessieClient.getTreeApi().getReferenceByName(s).getHash());
      } catch (BaseNessieClientServerException e) {
        // pass
      }
    }
    nessieClient.getTreeApi().createReference(Branch.of("main", null));
  }

  @Test
  void testCreateBranchInExists() throws NessieNotFoundException {

    List<Object[]> result = sql("CREATE BRANCH %s IN nessie", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    result = sql("CREATE BRANCH IF NOT EXISTS %s IN nessie", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    assertThatThrownBy(() -> sql("CREATE BRANCH %s IN nessie", refName))
        .isInstanceOf(NessieConflictException.class)
        .hasMessage("A reference of name [testBranch] already exists.");
    result = sql("DROP BRANCH %s IN nessie", refName);
    assertEquals("deleted branch", row("OK"), result);
  }

  @Test
  void testCreateBranchIn() throws NessieNotFoundException {

    List<Object[]> result = sql("CREATE BRANCH %s IN nessie", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    result = sql("DROP BRANCH %s IN nessie", refName);
    assertEquals("deleted branch", row("OK"), result);
  }

  @Test
  void testCreateTagIn() throws NessieNotFoundException {
    List<Object[]> result = sql("CREATE TAG %s IN nessie", refName);
    assertEquals("created tag", row("Tag", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Tag.of(refName, hash));
    result = sql("DROP TAG %s IN nessie", refName);
    assertEquals("deleted tag", row("OK"), result);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void testCreateBranchInAs() throws NessieNotFoundException {
    List<Object[]> result = sql("CREATE BRANCH %s IN nessie AS main", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    result = sql("DROP BRANCH %s IN nessie", refName);
    assertEquals("deleted branch", row("OK"), result);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Test
  void testCreateTagInAs() throws NessieNotFoundException {
    List<Object[]> result = sql("CREATE TAG %s IN nessie AS main", refName);
    assertEquals("created tag", row("Tag", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Tag.of(refName, hash));
    result = sql("LIST REFERENCES IN nessie");
    List<Object[]> listResult = new ArrayList<>();
    listResult.add(row("Branch", "main", hash));
    listResult.add(row("Tag", refName, hash));
    assertEquals("created branch", listResult, result);
    result = sql("DROP TAG %s IN nessie", refName);
    assertEquals("deleted tag", row("OK"), result);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void testCreateBranch() throws NessieNotFoundException {
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    List<Object[]> result = sql("CREATE BRANCH %s", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));
    result = sql("DROP BRANCH %s", refName);
    assertEquals("deleted branch", row("OK"), result);
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void testCreateTag() throws NessieNotFoundException {
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    List<Object[]> result = sql("CREATE TAG %s", refName);
    assertEquals("created branch", row("Tag", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Tag.of(refName, hash));
    result = sql("LIST REFERENCES");
    assertThat(result)
        .containsExactlyInAnyOrder(row("Tag", refName, hash), row("Branch", "main", hash));
    result = sql("DROP TAG %s", refName);
    assertEquals("deleted branch", row("OK"), result);
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void useShowReferencesIn() throws NessieNotFoundException {
    List<Object[]> result = sql("CREATE BRANCH %s IN nessie AS main", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    result = sql("USE REFERENCE %s IN nessie", refName);
    assertEquals("use branch", row("Branch", refName, hash), result);
    result = sql("SHOW REFERENCE IN nessie");
    assertEquals("show branch", row("Branch", refName, hash), result);

    result = sql("DROP BRANCH %s IN nessie", refName);
    assertEquals("deleted branch", row("OK"), result);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void useShowReferencesAt() throws NessieNotFoundException {
    List<Object[]> result = sql("CREATE BRANCH %s IN nessie AS main", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    result = sql("USE REFERENCE %s AT %s IN nessie ", refName, "`2012-06-01T14:14:14`");
    assertEquals("use branch", row("Branch", refName, hash), result);
    result = sql("SHOW REFERENCE IN nessie");
    assertEquals("show branch", row("Branch", refName, hash), result);

    result = sql("DROP BRANCH %s IN nessie", refName);
    assertEquals("deleted branch", row("OK"), result);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
  }

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void useShowReferences() throws NessieNotFoundException {
    List<Object[]> result = sql("CREATE BRANCH %s IN nessie AS main", refName);
    assertEquals("created branch", row("Branch", refName, hash), result);
    assertThat(nessieClient.getTreeApi().getReferenceByName(refName))
        .isEqualTo(Branch.of(refName, hash));

    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    result = sql("USE REFERENCE %s", refName);
    assertEquals("use branch", row("Branch", refName, hash), result);
    result = sql("SHOW REFERENCE");
    assertEquals("show branch", row("Branch", refName, hash), result);

    result = sql("DROP BRANCH %s IN nessie", refName);
    assertEquals("deleted branch", row("OK"), result);
    assertThatThrownBy(() -> nessieClient.getTreeApi().getReferenceByName(refName))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("Unable to find reference [testBranch].");
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);
  }

  @Test
  void mergeReferencesIntoMain() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList = commitAndReturnLog(refName);

    sql("MERGE BRANCH %s INTO main IN nessie", refName);
    List<Object[]> result = sql("SHOW LOG main IN nessie", refName);
    // here we are skipping commit time as its variable
    assertEquals(
        "log",
        result.stream().map(ITNessieStatements::convert).collect(Collectors.toList()),
        resultList);
  }

  @Test
  void mergeReferencesIn() throws NessieConflictException, NessieNotFoundException {
    List<Object[]> resultList = commitAndReturnLog(refName);

    sql("MERGE BRANCH %s IN nessie", refName);
    List<Object[]> result = sql("SHOW LOG main IN nessie", refName);
    // here we are skipping commit time as its variable
    assertEquals(
        "log",
        result.stream().map(ITNessieStatements::convert).collect(Collectors.toList()),
        resultList);
  }

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void mergeReferences() throws NessieConflictException, NessieNotFoundException {
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    List<Object[]> resultList = commitAndReturnLog(refName);
    sql("USE REFERENCE %s", refName);
    sql("MERGE BRANCH");
    List<Object[]> result = sql("SHOW LOG %s", refName);
    assertEquals(
        "log",
        result.stream().map(ITNessieStatements::convert).collect(Collectors.toList()),
        resultList);
    spark.sessionState().catalogManager().setCurrentCatalog(catalog);
  }

  @Test
  void showLogIn() throws NessieConflictException, NessieNotFoundException, AnalysisException {
    List<Object[]> resultList = commitAndReturnLog(refName);
    List<Object[]> result = sql("SHOW LOG %s IN nessie", refName);
    // here we are skipping commit time as its variable
    assertEquals(
        "log",
        result.stream().map(ITNessieStatements::convert).collect(Collectors.toList()),
        resultList);

    // test to ensure property map is correctly encoded by Spark
    spark.sql(String.format("SHOW LOG %s IN nessie", refName)).createTempView("nessie_log");
    result =
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
            .collect(Collectors.toList());

    assertEquals(
        "log",
        result.stream().map(ITNessieStatements::convert).collect(Collectors.toList()),
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
    sql("CREATE BRANCH %s IN nessie", branch);
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

  @Disabled("until release of 0.12.0 of iceberg")
  @Test
  void showLog() throws NessieConflictException, NessieNotFoundException {
    String catalog = spark.sessionState().catalogManager().currentCatalog().name();
    spark.sessionState().catalogManager().setCurrentCatalog("nessie");
    List<Object[]> resultList = commitAndReturnLog(refName);
    List<Object[]> result = sql("SHOW LOG %s", refName);

    // here we are skipping commit time as its variable
    assertEquals(
        "log",
        result.stream().map(ITNessieStatements::convert).collect(Collectors.toList()),
        resultList);
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
