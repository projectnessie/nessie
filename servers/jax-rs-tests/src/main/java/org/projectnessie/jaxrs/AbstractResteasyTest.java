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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableTransplant;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.Tag;

public abstract class AbstractResteasyTest {
  public static final String COMMA_VALID_HASH_1 =
      ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  protected static String basePath = "/api/v1/";

  private static RequestSpecification rest() {
    return given().when().basePath(basePath).contentType(ContentType.JSON);
  }

  private static HttpClient theClient;

  private static HttpRequest client() {
    HttpClient c = theClient;
    if (c == null) {
      ObjectMapper mapper = new ObjectMapper();
      URI uri = URI.create(RestAssured.baseURI).resolve(basePath);
      if (RestAssured.port != RestAssured.UNDEFINED_PORT) {
        try {
          uri =
              new URI(
                  uri.getScheme(),
                  uri.getUserInfo(),
                  uri.getHost(),
                  RestAssured.port,
                  uri.getPath(),
                  uri.getQuery(),
                  uri.getFragment());
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }
      c =
          HttpClient.builder()
              .setBaseUri(uri)
              .setObjectMapper(mapper)
              .addResponseFilter(new NessieHttpResponseFilter(mapper))
              .build();
      theClient = c;
    }
    return c.newRequest();
  }

  @BeforeEach
  public void enableLogging() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @Test
  public void testBasic() {
    int preSize =
        rest()
            .get("trees")
            .then()
            .statusCode(200)
            .extract()
            .as(ReferencesResponse.class)
            .getReferences()
            .size();

    rest().get("trees/tree/mainx").then().statusCode(404);
    rest().body(Branch.of("mainx", null)).post("trees/tree").then().statusCode(200);

    ReferencesResponse references =
        rest().get("trees").then().statusCode(200).extract().as(ReferencesResponse.class);
    Assertions.assertEquals(preSize + 1, references.getReferences().size());

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

    IcebergTable table = IcebergTable.of("/the/directory/over/there", 42, 42, 42, 42);

    Branch commitResponse =
        rest()
            .body(
                ImmutableOperations.builder()
                    .addOperations(
                        ImmutablePut.builder()
                            .key(ContentKey.of("xxx", "test"))
                            .content(table)
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
              .key(ContentKey.of("item", Integer.toString(i)))
              .content(IcebergTable.of("/the/directory/over/there/" + i, 42, 42, 42, 42))
              .build();
    }
    updates[10] =
        ImmutablePut.builder()
            .key(ContentKey.of("xxx", "test"))
            .content(IcebergTable.of("/the/directory/over/there/has/been/moved", 42, 42, 42, 42))
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
    Assertions.assertEquals(updates[10].getContent(), res.body().as(Content.class));

    IcebergTable currentTable = table;
    table =
        IcebergTable.of(
            "/the/directory/over/there/has/been/moved/again", 42, 42, 42, 42, table.getId());

    Branch b2 = rest().get("trees/tree/test").as(Branch.class);
    rest()
        .body(
            ImmutableOperations.builder()
                .addOperations(
                    ImmutablePut.builder()
                        .key(ContentKey.of("xxx", "test"))
                        .content(table)
                        .expectedContent(currentTable)
                        .build())
                .commitMeta(CommitMeta.fromMessage(""))
                .build())
        .queryParam("expectedHash", b2.getHash())
        .post("trees/branch/{branch}/commit", b2.getName())
        .then()
        .statusCode(200)
        .extract()
        .as(Branch.class);
    Content returned =
        rest()
            .queryParam("ref", "test")
            .get("contents/xxx.test")
            .then()
            .statusCode(200)
            .extract()
            .as(Content.class);
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
    Assertions.assertEquals(3, log.getLogEntries().size());

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

  @AfterAll
  public static void tearDownClient() {
    theClient = null;
  }

  private Branch commit(String contentId, Branch branch, String contentKey, String metadataUrl) {
    return commit(contentId, branch, contentKey, metadataUrl, "nessieAuthor", null);
  }

  private Branch commit(
      String contentId,
      Branch branch,
      String contentKey,
      String metadataUrl,
      String author,
      String expectedMetadataUrl) {
    Operations contents =
        ImmutableOperations.builder()
            .addOperations(
                (expectedMetadataUrl != null)
                    ? Put.of(
                        ContentKey.of(contentKey),
                        IcebergTable.of(metadataUrl, 42, 42, 42, 42, contentId),
                        IcebergTable.of(expectedMetadataUrl, 42, 42, 42, 42, contentId))
                    : Put.of(
                        ContentKey.of(contentKey),
                        IcebergTable.of(metadataUrl, 42, 42, 42, 42, contentId)))
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

  private Branch commit(ContentKey contentKey, Content content, Branch branch, String author) {
    Operations contents =
        ImmutableOperations.builder()
            .addOperations(Put.of(contentKey, content))
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
    return rest().body(test).post("trees/tree").then().statusCode(200).extract().as(Branch.class);
  }

  @Test
  public void testOptimisticLocking() {
    makeBranch("test3");
    Branch b1 = getBranch("test3");
    String contentId = "cid-test-opt-lock";
    String newHash = commit(contentId, b1, "xxx.test", "/the/directory/over/there").getHash();
    Assertions.assertNotEquals(b1.getHash(), newHash);

    Branch b2 = getBranch("test3");
    newHash =
        commit(
                contentId,
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
                contentId,
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
    String contentId = "cid-test-log-filtering";
    for (int i = 0; i < numCommits; i++) {
      String newHash =
          commit(
                  contentId,
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
    assertThat(log.getLogEntries()).hasSize(numCommits);
    Instant firstCommitTime =
        log.getLogEntries().get(log.getLogEntries().size() - 1).getCommitMeta().getCommitTime();
    Instant lastCommitTime = log.getLogEntries().get(0).getCommitMeta().getCommitTime();
    assertThat(firstCommitTime).isNotNull();
    assertThat(lastCommitTime).isNotNull();

    String author = "author-1";
    log =
        rest()
            .queryParam("filter", String.format("commit.author=='%s'", author))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getLogEntries()).hasSize(1);
    assertThat(log.getLogEntries().get(0).getCommitMeta().getAuthor()).isEqualTo(author);

    log =
        rest()
            .queryParam(
                "filter",
                String.format("timestamp(commit.commitTime) > timestamp('%s')", firstCommitTime))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getLogEntries()).hasSize(numCommits - 1);
    log.getLogEntries()
        .forEach(
            commit -> assertThat(commit.getCommitMeta().getCommitTime()).isAfter(firstCommitTime));

    log =
        rest()
            .queryParam(
                "filter",
                String.format("timestamp(commit.commitTime) < timestamp('%s')", lastCommitTime))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getLogEntries()).hasSize(numCommits - 1);
    log.getLogEntries()
        .forEach(
            commit -> assertThat(commit.getCommitMeta().getCommitTime()).isBefore(lastCommitTime));

    log =
        rest()
            .queryParam(
                "filter",
                String.format(
                    "timestamp(commit.commitTime) > timestamp('%s') && timestamp(commit.commitTime) < timestamp('%s')",
                    firstCommitTime, lastCommitTime))
            .get(String.format("trees/tree/%s/log", branchName))
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class);
    assertThat(log.getLogEntries()).hasSize(1);
    assertThat(log.getLogEntries().get(0).getCommitMeta().getCommitTime())
        .isBefore(lastCommitTime)
        .isAfter(firstCommitTime);
  }

  @Test
  public void testGetContent() {
    Branch branch = makeBranch("content-test");
    IcebergTable table = IcebergTable.of("content-table1", 42, 42, 42, 42);

    commit(table.getId(), branch, "key1", table.getMetadataLocation());

    Content content =
        rest()
            .queryParam("ref", branch.getName())
            .queryParam("hashOnRef", branch.getHash())
            .get(String.format("contents/%s", "key1"))
            .then()
            .statusCode(200)
            .extract()
            .as(Content.class);

    assertThat(content).isEqualTo(table);
  }

  @Test
  public void testGetDiff() {
    Branch fromBranch = makeBranch("getdiff-test-from");
    Branch toBranch = makeBranch("getdiff-test-to");
    IcebergTable fromTable = IcebergTable.of("content-table", 42, 42, 42, 42);
    IcebergTable toTable = IcebergTable.of("content-table", 43, 43, 43, 43);

    ContentKey contentKey = ContentKey.of("key1");
    commit(contentKey, fromTable, fromBranch, "diffAuthor");
    commit(contentKey, toTable, toBranch, "diffAuthor2");

    DiffResponse diffResponse =
        rest()
            .get(String.format("diffs/%s...%s", fromBranch.getName(), toBranch.getName()))
            .then()
            .statusCode(200)
            .extract()
            .as(DiffResponse.class);

    assertThat(diffResponse).isNotNull();
    assertThat(diffResponse.getDiffs()).hasSize(1);
    DiffEntry diff = diffResponse.getDiffs().get(0);
    assertThat(diff.getKey()).isEqualTo(contentKey);
    assertThat(diff.getFrom()).isEqualTo(fromTable);
    assertThat(diff.getTo()).isEqualTo(toTable);
  }

  @Test
  public void testGetRefLog() {
    Branch branch = makeBranch("branch-temp");
    IcebergTable table = IcebergTable.of("content-table", 42, 42, 42, 42);

    ContentKey contentKey = ContentKey.of("key1");
    commit(contentKey, table, branch, "code");

    RefLogResponse refLogResponse =
        rest().get("reflogs").then().statusCode(200).extract().as(RefLogResponse.class);

    assertThat(refLogResponse.getLogEntries().get(0).getOperation()).isEqualTo("COMMIT");
    assertThat(refLogResponse.getLogEntries().get(0).getRefName()).isEqualTo("branch-temp");
    assertThat(refLogResponse.getLogEntries().get(1).getOperation()).isEqualTo("CREATE_REFERENCE");
    assertThat(refLogResponse.getLogEntries().get(1).getRefName()).isEqualTo("branch-temp");

    RefLogResponse refLogResponse1 =
        rest()
            .queryParam("endHash", refLogResponse.getLogEntries().get(1).getRefLogId())
            .get("reflogs")
            .then()
            .statusCode(200)
            .extract()
            .as(RefLogResponse.class);
    assertThat(refLogResponse1.getLogEntries().get(0).getRefLogId())
        .isEqualTo(refLogResponse.getLogEntries().get(1).getRefLogId());
  }

  @ParameterizedTest
  @CsvSource({
    "x/" + COMMA_VALID_HASH_1,
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  public void invalidBranchNames(String invalidBranchName, String validHash) {
    ContentKey key = ContentKey.of("x");
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg = ".operations.operations: size must be between 1 and 2147483647";

    Branch defaultBranch = client().path("trees/tree").get().readEntity(Branch.class);

    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}/commit")
                            .post(
                                ImmutableOperations.builder()
                                    .commitMeta(CommitMeta.fromMessage(""))
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".referenceName: " + REF_NAME_MESSAGE)
                .hasMessageContaining(opsCountMsg),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteReference.referenceName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("startHash", validHash)
                            .path("trees/tree/{referenceName}/log")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getCommitLog.ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("hashOnRef", validHash)
                            .path("trees/tree/{referenceName}/entries")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getEntries.refName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceName", invalidBranchName)
                            .path("trees/tree/{referenceName}")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    "getReferenceByName.params.refName: " + REF_NAME_OR_HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .put(tag))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("assignReference.referenceName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}/merge")
                            .post(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRef.referenceName: " + REF_NAME_MESSAGE)
                .hasMessageContaining("mergeRef.merge: must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteReference.referenceName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}/transplant")
                            .post(
                                ImmutableTransplant.builder()
                                    .fromRefName("main")
                                    .addHashesToTransplant(defaultBranch.getHash())
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("transplantCommits.referenceName: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .queryParam("ref", invalidBranchName)
                            .queryParam("hashOnRef", validHash)
                            .path("contents")
                            .post(
                                ImmutableGetMultipleContentsRequest.builder()
                                    .addRequestedKeys(key)
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .queryParam("ref", invalidBranchName)
                            .queryParam("hashOnRef", validHash)
                            .path("contents")
                            .post(
                                ImmutableGetMultipleContentsRequest.builder()
                                    .addRequestedKeys(key)
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".ref: " + REF_NAME_MESSAGE),
        () -> {
          assertThatThrownBy(
                  () ->
                      client()
                          .resolveTemplate("fromRef", invalidBranchName)
                          .resolveTemplate("toRef", "main")
                          .path("diffs/{fromRef}...{toRef}")
                          .get())
              .isInstanceOf(NessieBadRequestException.class)
              .hasMessageContaining("Bad Request (HTTP/400):")
              .hasMessageContaining(".fromRef: " + REF_NAME_MESSAGE);
        },
        () -> {
          if (!invalidBranchName.startsWith(".")) {
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("fromRef", "main")
                            .resolveTemplate("toRef", invalidBranchName)
                            .path("diffs/{fromRef}...{toRef}")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".toRef: " + REF_NAME_MESSAGE);
          }
        });
  }

  @ParameterizedTest
  @CsvSource({
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  public void invalidHashes(String invalidHashIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidHash = invalidHashIn != null ? invalidHashIn : "";

    String validBranchName = "hello";
    ContentKey key = ContentKey.of("x");
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg = ".operations.operations: size must be between 1 and 2147483647";

    Branch defaultBranch = client().path("trees/tree").get().readEntity(Branch.class);

    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .path("trees/{referenceType}/{referenceName}/commit")
                            .post(
                                ImmutableOperations.builder()
                                    .commitMeta(CommitMeta.fromMessage(""))
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE)
                .hasMessageContaining(opsCountMsg),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteReference.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .put(tag))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("assignReference.oldHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .path("trees/{referenceType}/{referenceName}/merge")
                            .post(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRef.merge: must not be null")
                .hasMessageContaining("mergeRef.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("deleteReference.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "branch")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .path("trees/{referenceType}/{referenceName}/transplant")
                            .post(
                                ImmutableTransplant.builder()
                                    .fromRefName("main")
                                    .addHashesToTransplant(defaultBranch.getHash())
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("transplantCommits.hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .queryParam("ref", invalidHash)
                            .path("contents")
                            .post(ImmutableGetMultipleContentsRequest.builder().build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    ".request.requestedKeys: size must be between 1 and 2147483647")
                .hasMessageContaining(".ref: " + REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .queryParam("ref", validBranchName)
                            .queryParam("hashOnRef", invalidHash)
                            .path("contents")
                            .post(ImmutableGetMultipleContentsRequest.builder().build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    ".request.requestedKeys: size must be between 1 and 2147483647")
                .hasMessageContaining(".hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .queryParam("ref", validBranchName)
                            .queryParam("hashOnRef", invalidHash)
                            .path("contents")
                            .post(
                                ImmutableGetMultipleContentsRequest.builder()
                                    .addRequestedKeys(key)
                                    .build()))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("startHash", invalidHash)
                            .path("trees/tree/{referenceName}/log")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getCommitLog.params.startHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("endHash", invalidHash)
                            .path("trees/tree/{referenceName}/log")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getCommitLog.params.endHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("hashOnRef", invalidHash)
                            .path("trees/tree/{referenceName}/entries")
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("getEntries.params.hashOnRef: " + HASH_MESSAGE));
  }

  @ParameterizedTest
  @CsvSource({
    "" + COMMA_VALID_HASH_1,
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  public void invalidTags(String invalidTagNameIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidTagName = invalidTagNameIn != null ? invalidTagNameIn : "";

    String validBranchName = "hello";
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag =
        "{\"type\": \"TAG\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String branch =
        "{\"type\": \"BRANCH\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String different =
        "{\"type\": \"FOOBAR\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .put(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessage("Bad Request (HTTP/400): assignReference.assignTo: must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .put(tag))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Cannot construct instance of "
                        + "`org.projectnessie.model.ImmutableTag`, problem: "
                        + REF_NAME_MESSAGE
                        + " - but was: "
                        + invalidTagName
                        + "\n"),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .put(branch))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith("Bad Request (HTTP/400): Cannot construct instance of ")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        client()
                            .resolveTemplate("referenceType", "tag")
                            .resolveTemplate("referenceName", validBranchName)
                            .queryParam("expectedHash", validHash)
                            .path("trees/{referenceType}/{referenceName}")
                            .put(different))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                        + "`org.projectnessie.model.Reference`: known type ids = ["));
  }
}
