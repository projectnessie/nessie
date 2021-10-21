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
package org.projectnessie.jaxrs;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public class AbstractResteasyTest {

  protected static String basePath = "/api/v1/";

  @BeforeEach
  public void enableLogging() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Test
  public void testBasic() {
    int preSize = rest().get("trees").then().statusCode(200).extract().as(Reference[].class).length;

    rest().get("trees/tree/mainx").then().statusCode(404);
    rest().body(Branch.of("mainx", null)).post("trees/tree").then().statusCode(200);

    Reference[] references =
        rest().get("trees").then().statusCode(200).extract().as(Reference[].class);
    Assertions.assertEquals(preSize + 1, references.length);

    Reference reference =
        rest().get("trees/tree/mainx").then().statusCode(200).extract().as(Reference.class);
    assertEquals("mainx", reference.getName());

    Branch newReference = ImmutableBranch.builder().hash(reference.getHash()).name("test").build();
    rest()
        .queryParam("expectedHash", reference.getHash())
        .body(Branch.of("test", null))
        .post("trees/tree")
        .then()
        .statusCode(200);
    assertEquals(
        newReference,
        rest().get("trees/tree/test").then().statusCode(200).extract().as(Branch.class));

    IcebergTable table = IcebergTable.of("/the/directory/over/there", "x");

    Branch commitResponse =
        rest()
            .body(
                ImmutableOperations.builder()
                    .addOperations(
                        ImmutablePut.builder()
                            .key(ContentsKey.of("xxx", "test"))
                            .contents(table)
                            .build())
                    .commitMeta(CommitMeta.fromMessage(""))
                    .build())
            .queryParam("expectedHash", newReference.getHash())
            .post("trees/branch/{branch}/commit", newReference.getName())
            .then()
            .statusCode(200)
            .extract()
            .as(Branch.class);
    Assertions.assertNotEquals(newReference.getHash(), commitResponse.getHash());

    Put[] updates = new Put[11];
    for (int i = 0; i < 10; i++) {
      updates[i] =
          ImmutablePut.builder()
              .key(ContentsKey.of("item", Integer.toString(i)))
              .contents(IcebergTable.of("/the/directory/over/there/" + i, "x"))
              .build();
    }
    updates[10] =
        ImmutablePut.builder()
            .key(ContentsKey.of("xxx", "test"))
            .contents(IcebergTable.of("/the/directory/over/there/has/been/moved", "x"))
            .build();

    Reference branch = rest().get("trees/tree/test").as(Reference.class);
    Operations contents =
        ImmutableOperations.builder()
            .addOperations(updates)
            .commitMeta(CommitMeta.fromMessage(""))
            .build();

    commitResponse =
        rest()
            .body(contents)
            .queryParam("expectedHash", branch.getHash())
            .post("trees/branch/{branch}/commit", branch.getName())
            .then()
            .statusCode(200)
            .extract()
            .as(Branch.class);
    Assertions.assertNotEquals(branch.getHash(), commitResponse.getHash());

    Response res =
        rest().queryParam("ref", "test").get("contents/xxx.test").then().extract().response();
    Assertions.assertEquals(updates[10].getContents(), res.body().as(Contents.class));

    IcebergTable currentTable = table;
    table = IcebergTable.of("/the/directory/over/there/has/been/moved/again", "x", table.getId());

    Branch b2 = rest().get("trees/tree/test").as(Branch.class);
    rest()
        .body(
            ImmutableOperations.builder()
                .addOperations(
                    ImmutablePut.builder()
                        .key(ContentsKey.of("xxx", "test"))
                        .contents(table)
                        .expectedContents(currentTable)
                        .build())
                .commitMeta(CommitMeta.fromMessage(""))
                .build())
        .queryParam("expectedHash", b2.getHash())
        .post("trees/branch/{branch}/commit", b2.getName())
        .then()
        .statusCode(200)
        .extract()
        .as(Branch.class);
    Contents returned =
        rest()
            .queryParam("ref", "test")
            .get("contents/xxx.test")
            .then()
            .statusCode(200)
            .extract()
            .as(Contents.class);
    Assertions.assertEquals(table, returned);

    Branch b3 = rest().get("trees/tree/test").as(Branch.class);
    rest()
        .body(Tag.of("tagtest", b3.getHash()))
        .queryParam("sourceRefName", b3.getName())
        .post("trees/tree")
        .then()
        .statusCode(200);

    assertThat(
            rest()
                .get("trees/tree/tagtest")
                .then()
                .statusCode(200)
                .extract()
                .body()
                .as(Tag.class)
                .getHash())
        .isEqualTo(b3.getHash());

    rest()
        .queryParam(
            "expectedHash",
            "0011223344556677889900112233445566778899001122334455667788990011"
                .substring(0, b2.getHash().length()))
        .delete("trees/tag/tagtest")
        .then()
        .statusCode(409);

    rest()
        .queryParam("expectedHash", b3.getHash())
        .delete("trees/tag/tagtest")
        .then()
        .statusCode(204);

    LogResponse log =
        rest().get("trees/tree/test/log").then().statusCode(200).extract().as(LogResponse.class);
    Assertions.assertEquals(3, log.getOperations().size());

    Branch b1 = rest().get("trees/tree/test").as(Branch.class);
    rest()
        .queryParam("expectedHash", b1.getHash())
        .delete("trees/branch/test")
        .then()
        .statusCode(204);
    Branch bx = rest().get("trees/tree/mainx").as(Branch.class);
    rest()
        .queryParam("expectedHash", bx.getHash())
        .delete("trees/branch/mainx")
        .then()
        .statusCode(204);
  }

  private static RequestSpecification rest() {
    return given().when().basePath(basePath).contentType(ContentType.JSON);
  }

  private Branch commit(String contentsId, Branch branch, String contentsKey, String metadataUrl) {
    return commit(contentsId, branch, contentsKey, metadataUrl, "nessieAuthor", null);
  }

  private Branch commit(
      String contentsId,
      Branch branch,
      String contentsKey,
      String metadataUrl,
      String author,
      String expectedMetadataUrl) {
    Operations contents =
        ImmutableOperations.builder()
            .addOperations(
                (expectedMetadataUrl != null)
                    ? Put.of(
                        ContentsKey.of(contentsKey),
                        IcebergTable.of(metadataUrl, "x", contentsId),
                        IcebergTable.of(expectedMetadataUrl, "x", contentsId))
                    : Put.of(
                        ContentsKey.of(contentsKey), IcebergTable.of(metadataUrl, "x", contentsId)))
            .commitMeta(CommitMeta.builder().author(author).message("").build())
            .build();
    return rest()
        .body(contents)
        .queryParam("expectedHash", branch.getHash())
        .post("trees/branch/{branch}/commit", branch.getName())
        .then()
        .statusCode(200)
        .extract()
        .as(Branch.class);
  }

  private Branch getBranch(String name) {
    return rest().get("trees/tree/{name}", name).then().statusCode(200).extract().as(Branch.class);
  }

  private Branch makeBranch(String name) {
    Branch test = ImmutableBranch.builder().name(name).build();
    rest().body(test).post("trees/tree").then().statusCode(200);
    return test;
  }

  @Test
  public void testOptimisticLocking() {
    makeBranch("test3");
    Branch b1 = getBranch("test3");
    String contentsId = "cid-test-opt-lock";
    String newHash = commit(contentsId, b1, "xxx.test", "/the/directory/over/there").getHash();
    Assertions.assertNotEquals(b1.getHash(), newHash);

    Branch b2 = getBranch("test3");
    newHash =
        commit(
                contentsId,
                b2,
                "xxx.test",
                "/the/directory/over/there/has/been/moved",
                "i",
                "/the/directory/over/there")
            .getHash();
    Assertions.assertNotEquals(b2.getHash(), newHash);

    Branch b3 = getBranch("test3");
    newHash =
        commit(
                contentsId,
                b3,
                "xxx.test",
                "/the/directory/over/there/has/been/moved/again",
                "me",
                "/the/directory/over/there/has/been/moved")
            .getHash();
    Assertions.assertNotEquals(b3.getHash(), newHash);
  }

  @Test
  public void testLogFiltering() {
    String branchName = "logFiltering";
    makeBranch(branchName);
    Branch branch = getBranch(branchName);
    int numCommits = 3;
    String contentsId = "cid-test-log-filtering";
    for (int i = 0; i < numCommits; i++) {
      String newHash =
          commit(
                  contentsId,
                  branch,
                  "xxx.test",
                  "/the/directory/over/there",
                  "author-" + i,
                  i > 0 ? "/the/directory/over/there" : null)
              .getHash();
      assertThat(newHash).isNotEqualTo(branch.getHash());
      branch = getBranch(branchName);
    }
    LogResponse log =
        rest()
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getOperations()).hasSize(numCommits);
    Instant firstCommitTime =
        log.getOperations().get(log.getOperations().size() - 1).getCommitTime();
    Instant lastCommitTime = log.getOperations().get(0).getCommitTime();
    assertThat(firstCommitTime).isNotNull();
    assertThat(lastCommitTime).isNotNull();

    String author = "author-1";
    log =
        rest()
            .queryParam("query_expression", String.format("commit.author=='%s'", author))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getOperations()).hasSize(1);
    assertThat(log.getOperations().get(0).getAuthor()).isEqualTo(author);

    log =
        rest()
            .queryParam(
                "query_expression",
                String.format("timestamp(commit.commitTime) > timestamp('%s')", firstCommitTime))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getOperations()).hasSize(numCommits - 1);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getCommitTime()).isAfter(firstCommitTime));

    log =
        rest()
            .queryParam(
                "query_expression",
                String.format("timestamp(commit.commitTime) < timestamp('%s')", lastCommitTime))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getOperations()).hasSize(numCommits - 1);
    log.getOperations()
        .forEach(commit -> assertThat(commit.getCommitTime()).isBefore(lastCommitTime));

    log =
        rest()
            .queryParam(
                "query_expression",
                String.format(
                    "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                    firstCommitTime, lastCommitTime))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getOperations()).hasSize(1);
    assertThat(log.getOperations().get(0).getCommitTime())
        .isBefore(lastCommitTime)
        .isAfter(firstCommitTime);
  }
}
