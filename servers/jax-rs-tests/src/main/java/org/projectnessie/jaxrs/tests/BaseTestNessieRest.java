/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.jaxrs.tests;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.Tag;

/** REST specific tests. */
public abstract class BaseTestNessieRest extends BaseTestNessieApi {

  protected URI clientUri;

  @BeforeEach
  public void enableLogging() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  @BeforeEach
  void setupRestUri(@NessieClientUri URI uri) {
    clientUri = uri;
    RestAssured.baseURI = uri.toString();
    RestAssured.port = uri.getPort();
  }

  @BeforeAll
  static void setupRestAssured() {
    RestAssured.requestSpecification =
        new RequestSpecBuilder()
            .setContentType(ContentType.JSON)
            .setAccept(ContentType.JSON)
            .build();
  }

  protected static RequestSpecification rest() {
    return given().when().baseUri(RestAssured.baseURI).basePath("").contentType(ContentType.JSON);
  }

  private static Content withoutId(Content content) {
    if (content instanceof IcebergTable) {
      return IcebergTable.builder().from(content).id(null).build();
    }
    throw new IllegalArgumentException("Expected IcebergTable, got " + content);
  }

  private ValidatableResponse prepareCommitV1(
      ContentKey contentKey,
      Content content,
      Branch branch,
      int clientSpec,
      boolean buildMissingNamespaces) {
    ImmutableOperations.Builder contents =
        ImmutableOperations.builder()
            .commitMeta(CommitMeta.builder().author("test author").message("").build());
    if (buildMissingNamespaces) {
      if (contentKey.getElementCount() > 1) {
        contents.addOperations(Put.of(contentKey.getParent(), contentKey.getNamespace()));
      }
      if (contentKey.getElementCount() > 2) {
        contents.addOperations(
            Put.of(contentKey.getParent().getParent(), contentKey.getParent().getNamespace()));
      }
    }
    contents.addOperations(Put.of(contentKey, content));
    RequestSpecification resp = rest().body(contents.build());
    if (clientSpec > 0) {
      resp = resp.header("Nessie-Client-Spec", clientSpec);
    }
    return resp.queryParam("expectedHash", branch.getHash())
        .post("trees/branch/{branch}/commit", branch.getName())
        .then();
  }

  private Branch commitV1(ContentKey contentKey, Content content, Branch branch) {
    return prepareCommitV1(contentKey, content, branch, 2, true)
        .statusCode(200)
        .extract()
        .as(Branch.class);
  }

  private Branch createBranchV1(String name) {
    Branch main = rest().get("trees/tree").then().statusCode(200).extract().as(Branch.class);
    Branch test = ImmutableBranch.builder().name(name).hash(main.getHash()).build();
    return rest().body(test).post("trees/tree").then().statusCode(200).extract().as(Branch.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "trees/not-there-using",
        "contents/not-there-using",
        "something/not-there-using",
        ""
      })
  public void testNotFoundUrls(String path) {
    rest()
        .filter(
            (req, resp, ctx) -> {
              req.removeHeader("Content-Type");
              return ctx.next(req, resp);
            })
        .get(path)
        .then()
        .statusCode(404);
    rest()
        .filter(
            (req, resp, ctx) -> {
              req.removeHeader("Content-Type");
              return ctx.next(req, resp);
            })
        .head(path)
        .then()
        .statusCode(404);
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V1})
  public void testReferenceConflictDetailsV1() {
    ContentKey key = ContentKey.of("namespace", "foo");
    IcebergTable table = IcebergTable.of("content-table1", 42, 42, 42, 42);

    Branch branch = createBranchV1("ref-conflicts");

    NessieError nessieError =
        prepareCommitV1(key, table, branch, 2, false)
            .statusCode(409)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError.getErrorCode()).isEqualTo(ErrorCode.REFERENCE_CONFLICT);
    soft.assertThat(nessieError.getErrorDetails())
        .isNotNull()
        .asInstanceOf(type(ReferenceConflicts.class))
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .hasSize(1)
        .extracting(Conflict::conflictType)
        .containsExactly(ConflictType.NAMESPACE_ABSENT);

    // Older Nessie clients must not receive the new `errorDetails` property.
    nessieError =
        prepareCommitV1(key, table, branch, 1, false)
            .statusCode(409)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError.getErrorCode()).isEqualTo(ErrorCode.REFERENCE_CONFLICT);
    soft.assertThat(nessieError.getErrorDetails()).isNull();

    // Older Nessie clients must not receive the new `errorDetails` property.
    nessieError =
        prepareCommitV1(key, table, branch, 0, false)
            .statusCode(409)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError.getErrorCode()).isEqualTo(ErrorCode.REFERENCE_CONFLICT);
    soft.assertThat(nessieError.getErrorDetails()).isNull();
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  public void testReferenceConflictDetailsV2() {
    ContentKey key = ContentKey.of("namespace", "foo");
    IcebergTable table = IcebergTable.of("content-table1", 42, 42, 42, 42);

    Branch branch = createBranchV2("ref-conflicts");

    NessieError nessieError =
        prepareCommitV2(branch, key, table, 2, false)
            .statusCode(409)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError.getErrorCode()).isEqualTo(ErrorCode.REFERENCE_CONFLICT);
    soft.assertThat(nessieError.getErrorDetails())
        .isNotNull()
        .asInstanceOf(type(ReferenceConflicts.class))
        .extracting(ReferenceConflicts::conflicts, list(Conflict.class))
        .hasSize(1)
        .extracting(Conflict::conflictType)
        .containsExactly(ConflictType.NAMESPACE_ABSENT);

    // Older Nessie clients must not receive the new `errorDetails` property.
    nessieError =
        prepareCommitV2(branch, key, table, 1, false)
            .statusCode(409)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError.getErrorCode()).isEqualTo(ErrorCode.REFERENCE_CONFLICT);
    soft.assertThat(nessieError.getErrorDetails()).isNull();

    // Older Nessie clients must not receive the new `errorDetails` property.
    nessieError =
        prepareCommitV2(branch, key, table, 0, false)
            .statusCode(409)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError.getErrorCode()).isEqualTo(ErrorCode.REFERENCE_CONFLICT);
    soft.assertThat(nessieError.getErrorDetails()).isNull();
  }

  @NessieApiVersions(versions = {NessieApiVersion.V1})
  @ParameterizedTest
  @CsvSource({
    "simple,name",
    "simple,dotted.txt",
    "dotted.prefix,name",
    "dotted.prefix,dotted.txt",
  })
  public void testGetContent(String ns, String name) throws JsonProcessingException {
    Branch branch = createBranchV1("content-test-" + UUID.randomUUID());
    IcebergTable table = IcebergTable.of("content-table1", 42, 42, 42, 42);

    ContentKey key = ContentKey.of(ns, name);
    branch = commitV1(key, table, branch);

    Content content =
        rest()
            .queryParam("ref", branch.getName())
            .queryParam("hashOnRef", branch.getHash())
            .get(String.format("contents/%s", key.toPathString()))
            .then()
            .statusCode(200)
            .extract()
            .as(Content.class);
    soft.assertThat(withoutId(content)).isEqualTo(table);

    String response =
        rest()
            .queryParam("ref", branch.getName())
            .queryParam("hashOnRef", branch.getHash())
            .body(GetMultipleContentsRequest.of(key))
            .post("contents")
            .then()
            .statusCode(200)
            .extract()
            .asString();
    // The "effectiveReference" attribute applies only to Nessie REST API v2
    // and should not be present in v1 responses.
    soft.assertThat(response).doesNotContain("effectiveReference");
    GetMultipleContentsResponse multi =
        new ObjectMapper().readValue(response, GetMultipleContentsResponse.class);
    soft.assertThat(withoutId(multi.toContentsMap().get(key))).isEqualTo(table);
    soft.assertThat(multi.getEffectiveReference()).isNull();
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V1})
  public void testGetUnknownContentType() {
    String nsName = "foo";
    String branchName = "unknown-content-type";
    String path = String.format("namespaces/namespace/%s/%s", branchName, nsName);

    createBranchV1(branchName);
    Namespace ns = Namespace.of("id");

    // Elicit 415 when PUTting a namespace with a content type not consumed by the server
    NessieError error =
        rest()
            .body(ns)
            .contentType(ContentType.TEXT)
            .put(path)
            .then()
            .statusCode(415)
            .extract()
            .as(NessieError.class);
    assertThat(error.getReason()).containsIgnoringCase("Unsupported Media Type");

    // Rerun the request, but with a supported content type (text/json)
    rest().body(ns).put(path).then().statusCode(200);
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V1})
  public void testNullMergePayload() {
    NessieError response =
        rest()
            .post("trees/branch/main/merge")
            .then()
            .statusCode(400)
            .extract()
            .as(NessieError.class);
    assertThat(response.getMessage()).contains(".merge: must not be null");
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V1})
  public void testNullTagPayload() {
    NessieError response =
        rest().put("trees/tag/newTag").then().statusCode(400).extract().as(NessieError.class);
    assertThat(response.getMessage()).contains(".assignTo: must not be null");
    assertThat(response.getMessage()).contains(".expectedHash: must not be null");
  }

  @NessieApiVersions(versions = {NessieApiVersion.V1})
  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "abc'",
        ".foo",
        "abc'def'..'blah",
        "abc'de..blah",
        "abc'de@{blah",
      })
  public void testInvalidTag(String invalidTagName) {
    String validHash = "0011223344556677";
    String validRefName = "hello";

    assertAll(
        () ->
            assertThat(
                    rest()
                        // Need the string-ified JSON representation of `Tag` here, because `Tag`
                        // itself performs validation.
                        .body(
                            "{\"type\": \"TAG\", \"name\": \""
                                + invalidTagName
                                + "\", \"hash\": \""
                                + validHash
                                + "\"}")
                        .put("trees/tag/" + validRefName)
                        .then()
                        .statusCode(400)
                        .extract()
                        .as(NessieError.class)
                        .getMessage())
                .contains(
                    "Cannot construct instance of `org.projectnessie.model.ImmutableTag`, problem: "
                        + REF_NAME_MESSAGE
                        + " - but was: "
                        + invalidTagName
                        + "\n"),
        () ->
            assertThat(
                    rest()
                        .body(
                            "{\"type\": \"TAG\", \"name\": \""
                                + invalidTagName
                                + "\", \"hash\": \""
                                + validHash
                                + "\"}")
                        .put("trees/tag/" + validRefName)
                        .then()
                        .statusCode(400)
                        .extract()
                        .as(NessieError.class)
                        .getMessage())
                .contains(
                    "Cannot construct instance of `org.projectnessie.model.ImmutableTag`, problem: "
                        + REF_NAME_MESSAGE),
        () ->
            assertThat(
                    rest()
                        .body(
                            "{\"type\": \"FOOBAR\", \"name\": \""
                                + validRefName
                                + "\", \"hash\": \""
                                + validHash
                                + "\"}")
                        .put("trees/tag/" + validRefName)
                        .then()
                        .statusCode(400)
                        .extract()
                        .as(NessieError.class)
                        .getMessage())
                .contains(
                    "Could not resolve type id 'FOOBAR' as a subtype of `org.projectnessie.model.Reference`"));
  }

  Branch createBranchV2(String branchName) {
    Reference main =
        rest()
            .get("trees/-")
            .then()
            .statusCode(200)
            .extract()
            .as(SingleReferenceResponse.class)
            .getReference();
    return createBranchV2(branchName, main);
  }

  Branch createBranchV2(String branchName, Reference ref) {
    return (Branch)
        rest()
            .queryParam("name", branchName)
            .queryParam("type", Reference.ReferenceType.BRANCH.name())
            .body(ref)
            .post("trees")
            .then()
            .statusCode(200)
            .extract()
            .as(SingleReferenceResponse.class)
            .getReference();
  }

  Tag createTagV2(String tagName, Reference ref) {
    return (Tag)
        rest()
            .queryParam("name", tagName)
            .queryParam("type", Reference.ReferenceType.TAG.name())
            .body(ref)
            .post("trees")
            .then()
            .statusCode(200)
            .extract()
            .as(SingleReferenceResponse.class)
            .getReference();
  }

  ValidatableResponse prepareCommitV2(
      Branch branch,
      ContentKey key,
      IcebergTable table,
      int clientSpec,
      boolean buildMissingNamespaces) {
    return prepareCommitV2(branch.toPathString(), key, table, clientSpec, buildMissingNamespaces);
  }

  ValidatableResponse prepareCommitV2(
      String ref,
      ContentKey key,
      IcebergTable table,
      int clientSpec,
      boolean buildMissingNamespaces) {
    ImmutableOperations.Builder ops =
        ImmutableOperations.builder().commitMeta(CommitMeta.fromMessage("test commit"));
    if (buildMissingNamespaces) {
      if (key.getElementCount() > 1) {
        ops.addOperations(Put.of(key.getParent(), key.getNamespace()));
      }
      if (key.getElementCount() > 2) {
        ops.addOperations(Put.of(key.getParent().getParent(), key.getParent().getNamespace()));
      }
    }
    ops.addOperations(Put.of(key, table));
    RequestSpecification resp = rest().body(ops.build());
    if (clientSpec > 0) {
      resp = resp.header("Nessie-Client-Spec", clientSpec);
    }
    return resp.post("trees/{ref}/history/commit", ref).then();
  }

  ValidatableResponse prepareMergeV2(
      String targetRefAndHash, String fromRefName, String fromHash, int clientSpec) {
    Map<String, String> body = new HashMap<>();
    body.put("fromRefName", fromRefName);
    body.put("fromHash", fromHash);
    RequestSpecification resp = rest().body(body);
    if (clientSpec > 0) {
      resp = resp.header("Nessie-Client-Spec", clientSpec);
    }
    return resp.post("trees/{ref}/history/merge", targetRefAndHash).then();
  }

  ValidatableResponse prepareTransplantV2(
      String targetRefAndHash,
      String fromRefName,
      List<String> hashesToTransplant,
      int clientSpec) {
    Map<String, Object> body = new HashMap<>();
    body.put("fromRefName", fromRefName);
    body.put("hashesToTransplant", hashesToTransplant);
    RequestSpecification resp = rest().body(body);
    if (clientSpec > 0) {
      resp = resp.header("Nessie-Client-Spec", clientSpec);
    }
    return resp.post("trees/{ref}/history/transplant", targetRefAndHash).then();
  }

  CommitResponse commitGetResponse(Branch branch, ContentKey key, IcebergTable table) {
    return prepareCommitV2(branch, key, table, 2, true)
        .statusCode(200)
        .extract()
        .as(CommitResponse.class);
  }

  Branch commitV2(Branch branch, ContentKey key, IcebergTable table) {
    return commitGetResponse(branch, key, table).getTargetBranch();
  }

  private Content getContentV2(Reference reference, ContentKey key) {
    return rest()
        .get("trees/{ref}/contents/{key}", reference.toPathString(), key.toPathString())
        .then()
        .statusCode(200)
        .extract()
        .as(ContentResponse.class)
        .getContent();
  }

  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @Test
  void testCommitMetaHeaders() {
    String message = "My own commit message";
    String author1 = "Me <me@me.internal>";
    String author2 = "I <me@me.internal>";
    String signedOffBy1 = "Myself <myself@me.internal>";
    String signedOffBy2 = "My other self <myself@me.internal>";

    Branch branch = createBranchV2("testCommitMetaHeaders");

    ImmutableOperations.Builder ops =
        ImmutableOperations.builder().commitMeta(CommitMeta.fromMessage("test commit"));
    ops.addOperations(Put.of(ContentKey.of("my-namespace"), Namespace.of("my-namespace")));
    rest()
        .body(ops.build())
        .header("Nessie-Commit-Message", message)
        .header("Nessie-Commit-Authors", author1, author2)
        .header("Nessie-Commit-SignedOffBy", signedOffBy1, signedOffBy2)
        .header("Nessie-Commit-Property-foo", "bar", "baz")
        .header("Nessie-Commit-Property-dog", "Elani")
        .post("trees/{ref}/history/commit", branch.toPathString())
        .then()
        .statusCode(200)
        .extract()
        .as(CommitResponse.class);

    List<LogResponse.LogEntry> commits =
        rest()
            .get("trees/{ref}/history", branch.getName())
            .then()
            .statusCode(200)
            .extract()
            .as(LogResponse.class)
            .getLogEntries();

    soft.assertThat(commits)
        .extracting(LogResponse.LogEntry::getCommitMeta)
        .allSatisfy(
            meta -> {
              soft.assertThat(meta.getAllAuthors())
                  .containsExactlyElementsOf(List.of(author1, author2));
              soft.assertThat(meta.getAllSignedOffBy())
                  .containsExactlyElementsOf(List.of(signedOffBy1, signedOffBy2));
              soft.assertThat(meta.getMessage()).isEqualTo(message);
              soft.assertThat(meta.getAllProperties())
                  .containsExactlyInAnyOrderEntriesOf(
                      Map.of("foo", List.of("bar", "baz"), "dog", List.of("Elani")));
            });
  }

  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @ParameterizedTest(name = "{displayName}[{index}]")
  @CsvSource({
    "simple1,testKey",
    "simple2,test.Key",
    "simple3,test\u001DKey",
    "simple4,test\u001Dnested.Key",
    "with/slash1,testKey",
    "with/slash2,test.Key",
    "with/slash3,test\u001DKey",
    "with/slash4,test\u001D.nested.Key",
  })
  void testGetSingleContent(String branchName, String encodedKey) {
    Branch branch = createBranchV2(branchName);
    ContentKey key = ContentKey.fromPathString(encodedKey);
    IcebergTable table = IcebergTable.of("test-location", 1, 2, 3, 4);
    branch = commitV2(branch, key, table);
    assertThat(getContentV2(branch, key))
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("test-location");
  }

  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @ParameterizedTest
  @ValueSource(strings = {"simple", "with/slash"})
  void testGetSeveralContents(String branchName) {
    Branch branch = createBranchV2(branchName);
    ContentKey key1 = ContentKey.of("test", "Key");
    ContentKey key2 = ContentKey.of("test.with.dot", "Key");
    IcebergTable table1 = IcebergTable.of("loc1", 1, 2, 3, 4);
    IcebergTable table2 = IcebergTable.of("loc2", 1, 2, 3, 4);
    branch = commitV2(branch, key1, table1);
    branch = commitV2(branch, key2, table2);

    EntriesResponse entries =
        rest()
            .get("trees/{ref}/entries", branch.toPathString())
            .then()
            .statusCode(200)
            .extract()
            .as(EntriesResponse.class);
    assertThat(entries.getEntries())
        .hasSize(4)
        .allSatisfy(e -> assertThat(e.getContentId()).isNotNull());

    Stream<MapEntry<ContentKey, String>> contents =
        rest()
            .queryParam("key", key1.toPathString(), key2.toPathString())
            .get("trees/{ref}/contents", branch.toPathString())
            .then()
            .statusCode(200)
            .extract()
            .as(GetMultipleContentsResponse.class)
            .getContents()
            .stream()
            .map(
                content ->
                    entry(
                        content.getKey(),
                        ((IcebergTable) content.getContent()).getMetadataLocation()));

    assertThat(contents).containsExactlyInAnyOrder(entry(key1, "loc1"), entry(key2, "loc2"));
  }

  /** Dedicated test for human-readable references in URL paths. */
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @Test
  void testBranchWithSlashInUrlPath(@NessieClientUri URI clientUri) throws IOException {
    Branch branch = createBranchV2("test/branch/name1");

    assertThat(
            rest()
                .get("trees/test/branch/name1@")
                .then()
                .statusCode(200)
                .extract()
                .as(SingleReferenceResponse.class)
                .getReference())
        .isEqualTo(branch);

    // The above request encodes `@` as `%40`, now try the same URL with a literal `@` char to
    // simulate handwritten `curl` requests.
    URL url = clientUri.resolve(clientUri.getPath() + "/trees/test/branch/name1@").toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertThat(conn.getResponseCode()).isEqualTo(200);
    conn.disconnect();
  }

  /** Dedicated test for human-readable default branch references in URL paths. */
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @Test
  void testDefaultBranchSpecInUrlPath() {
    Reference main =
        rest()
            .get("trees/-")
            .then()
            .statusCode(200)
            .extract()
            .as(SingleReferenceResponse.class)
            .getReference();
    assertThat(main.getName()).isEqualTo("main");

    Branch branch = createBranchV2("testDefaultBranchSpecInUrlPath");
    ContentKey key = ContentKey.of("test1");
    commitV2(branch, key, IcebergTable.of("loc", 1, 2, 3, 4));

    assertThat(
            rest()
                .get("trees/-/diff/{name}", branch.getName())
                .then()
                .statusCode(200)
                .extract()
                .as(DiffResponse.class)
                .getDiffs())
        .satisfiesExactly(e -> assertThat(e.getKey()).isEqualTo(key));

    assertThat(
            rest()
                .get("trees/{name}/diff/-", branch.getName())
                .then()
                .statusCode(200)
                .extract()
                .as(DiffResponse.class)
                .getDiffs())
        .satisfiesExactly(e -> assertThat(e.getKey()).isEqualTo(key));

    assertThat(
            rest()
                .get("trees/-/diff/-")
                .then()
                .statusCode(200)
                .extract()
                .as(DiffResponse.class)
                .getDiffs())
        .isEmpty();
  }

  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @Test
  public void testCommitMetaAttributes() {
    Branch branch = createBranchV2("testCommitMetaAttributes");
    commitV2(branch, ContentKey.of("test-key"), IcebergTable.of("meta", 1, 2, 3, 4));

    String response =
        rest()
            .get("trees/{ref}/history", branch.getName())
            .then()
            .statusCode(200)
            .extract()
            .asString();
    assertThat(response)
        .doesNotContain("\"author\"") // only for API v1
        .contains("\"authors\"")
        .doesNotContain("\"signedOffBy\"") // only for API v1
        .contains("allSignedOffBy");
  }

  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @Test
  public void referenceTypeInvalidValue() {
    NessieError nessieError =
        rest()
            .delete(
                "trees/test@2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d?type=X")
            .then()
            .statusCode(400)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError)
        .extracting(NessieError::getStatus, NessieError::getErrorCode)
        .containsExactly(400, ErrorCode.BAD_REQUEST);
    soft.assertThat(nessieError.getMessage())
        .contains("Reference type name must be either 'branch' or 'tag'");

    nessieError =
        rest()
            .body("{\"type\": \"TAG\", \"name\": \"name\", \"hash\": \"feedbeef\"}")
            .put(
                "trees/test@2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d?type=X")
            .then()
            .statusCode(400)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError)
        .extracting(NessieError::getStatus, NessieError::getErrorCode)
        .containsExactly(400, ErrorCode.BAD_REQUEST);
    soft.assertThat(nessieError.getMessage())
        .contains("Reference type name must be either 'branch' or 'tag'");

    nessieError =
        rest()
            .queryParam("name", "branch")
            .queryParam("type", "X")
            .post("trees")
            .then()
            .statusCode(400)
            .extract()
            .as(NessieError.class);
    soft.assertThat(nessieError)
        .extracting(NessieError::getStatus, NessieError::getErrorCode)
        .containsExactly(400, ErrorCode.BAD_REQUEST);
    soft.assertThat(nessieError.getMessage())
        .contains("Reference type name must be either 'branch' or 'tag'");
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  public void getRefByNameHashNotAllowed() {
    NessieError nessieError =
        rest().get("trees/main@12345678").then().statusCode(400).extract().as(NessieError.class);
    soft.assertThat(nessieError)
        .extracting(NessieError::getStatus, NessieError::getErrorCode)
        .containsExactly(400, ErrorCode.BAD_REQUEST);
    soft.assertThat(nessieError.getMessage())
        .contains("Hashes are not allowed when fetching a reference by name");
  }

  @NessieApiVersions(versions = {NessieApiVersion.V2})
  @Test
  public void assignAndDeleteRefWithoutType() throws Exception {
    Branch branch = createBranchV2("assignAndDeleteRefWithoutType");
    String initial = branch.getHash();
    branch = commitV2(branch, ContentKey.of("foo"), IcebergTable.of("a", 1, 2, 3, 4));

    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    Branch testBranch1 =
        (Branch)
            api.createReference()
                .sourceRefName(branch.getName())
                .reference(Branch.of("branch1", branch.getHash()))
                .create();
    Branch testBranch2 =
        (Branch)
            api.createReference()
                .sourceRefName(branch.getName())
                .reference(Branch.of("branch2", branch.getHash()))
                .create();
    Tag testTag1 =
        (Tag)
            api.createReference()
                .sourceRefName(branch.getName())
                .reference(Tag.of("tag1", branch.getHash()))
                .create();
    Tag testTag2 =
        (Tag)
            api.createReference()
                .sourceRefName(branch.getName())
                .reference(Tag.of("tag2", branch.getHash()))
                .create();

    soft.assertThat(
            rest()
                .body(
                    "{\"type\": \"BRANCH\", \"name\": \""
                        + testBranch1.getName()
                        + "\", \"hash\": \""
                        + initial
                        + "\"}")
                .put("trees/{name}@{hash}", testBranch1.getName(), testBranch1.getHash())
                .then()
                .statusCode(200)
                .extract()
                .as(SingleReferenceResponse.class))
        .extracting(SingleReferenceResponse::getReference)
        .isEqualTo(Branch.of(testBranch1.getName(), initial));
    soft.assertThat(
            rest()
                .delete("trees/{name}@{hash}", testBranch2.getName(), testBranch2.getHash())
                .then()
                .statusCode(200)
                .extract()
                .as(SingleReferenceResponse.class))
        .extracting(SingleReferenceResponse::getReference)
        .isEqualTo(testBranch2);

    soft.assertThat(
            rest()
                .body(
                    "{\"type\": \"TAG\", \"name\": \""
                        + testTag1.getName()
                        + "\", \"hash\": \""
                        + initial
                        + "\"}")
                .put("trees/{name}@{hash}", testTag1.getName(), testTag1.getHash())
                .then()
                .statusCode(200)
                .extract()
                .as(SingleReferenceResponse.class))
        .extracting(SingleReferenceResponse::getReference)
        .isEqualTo(Tag.of("tag1", initial));
    soft.assertThat(
            rest()
                .delete("trees/{name}@{hash}", testTag2.getName(), testTag2.getHash())
                .then()
                .statusCode(200)
                .extract()
                .as(SingleReferenceResponse.class))
        .extracting(SingleReferenceResponse::getReference)
        .isEqualTo(testTag2);
  }

  @Test
  @NessieApiVersions(versions = {NessieApiVersion.V2})
  public void problematicCharactersInContentKeys() throws Exception {
    Branch branch = createBranchV2("problematicCharactersInContentKeys");

    Map<ContentKey, IcebergTable> contents =
        Stream.of(
                ContentKey.of("abc.", "{def"),
                ContentKey.of("abc.", ".def"),
                ContentKey.of("abc.", "}def"),
                ContentKey.of("abc.", "[def"),
                ContentKey.of("abc.", "/def"),
                ContentKey.of("abc.", "%def"),
                ContentKey.of("abc.", "\\def"),
                ContentKey.of("/%", "#&&"),
                ContentKey.of("/%国", "国.国"),
                ContentKey.of("he\\llo"),
                ContentKey.of("he%llo"),
                ContentKey.of("he#llo", "world"),
                ContentKey.of("he&llo", "world"),
                ContentKey.of("he?llo", "world"),
                ContentKey.of("he/llo", "world"),
                ContentKey.of("hello."),
                ContentKey.of(".hello.~hello"),
                ContentKey.of(".hello", ".~hello"),
                ContentKey.of(".hello.", "~hello"),
                ContentKey.of(".hello.", ".~hello"),
                ContentKey.of(".hello.~", "~hello"),
                ContentKey.of("foo", "_ar", "baz"),
                ContentKey.of("foo", "{ar", "baz"),
                ContentKey.of("foo", "}ar", "baz"),
                ContentKey.of("foo", "[ar", "baz"))
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    k -> IcebergTable.of("meta-" + UUID.randomUUID(), 1, 2, 3, 4)));

    List<Namespace> namespaces =
        contents.keySet().stream()
            .flatMap(
                k ->
                    IntStream.range(1, k.getElementCount())
                        .mapToObj(l -> Namespace.of(k.getElements().subList(0, l))))
            .distinct()
            .collect(Collectors.toList());

    @SuppressWarnings("resource")
    NessieApiV2 api = apiV2();

    CommitMultipleOperationsBuilder commitNamespaces =
        api.commitMultipleOperations().branch(branch).commitMeta(fromMessage("create namespaces"));
    namespaces.forEach(n -> commitNamespaces.operation(Put.of(n.toContentKey(), n)));
    CommitResponse namespacesCommit = commitNamespaces.commitWithResponse();

    CommitMultipleOperationsBuilder commitContent =
        api.commitMultipleOperations()
            .branch(namespacesCommit.getTargetBranch())
            .commitMeta(fromMessage("create content"));
    contents.forEach((key, content) -> commitContent.operation(Put.of(key, content)));
    commitContent.commitWithResponse();

    contents.forEach(
        (key, expectedContent) -> {
          for (String keyRepresentation : List.of(key.toPathString(), key.toPathStringEscaped())) {

            soft.assertThatCode(
                    () -> {
                      ContentResponse response =
                          rest()
                              .get(
                                  "trees/{branch}/contents/{key}",
                                  branch.getName(),
                                  keyRepresentation)
                              .then()
                              .statusCode(200)
                              .extract()
                              .as(ContentResponse.class);
                      assertThat(response.getContent().withId(null)).isEqualTo(expectedContent);
                    })
                .describedAs("key %s - using URI path %s", key, keyRepresentation)
                .doesNotThrowAnyException();
          }
        });
  }
}
