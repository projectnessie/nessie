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
package org.projectnessie.jaxrs.tests;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SingleReferenceResponse;

// Resteasy requests in these tests are crafted for v2 REST paths.
@NessieApiVersions(versions = {NessieApiVersion.V2})
public abstract class AbstractResteasyV2Test {

  protected static String basePath = "/api/v2/";

  @BeforeEach
  public void enableLogging() {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
  }

  private static RequestSpecification rest() {
    return given()
        .when()
        .baseUri(RestAssured.baseURI)
        .basePath(basePath)
        .contentType(ContentType.JSON);
  }

  private Branch createBranch(String branchName) {
    // Note: no request body means creating the new branch from the HEAD of the default branch.
    return (Branch)
        rest()
            .queryParam("name", branchName)
            .queryParam("type", Reference.ReferenceType.BRANCH.name())
            .post("trees")
            .then()
            .statusCode(200)
            .extract()
            .as(SingleReferenceResponse.class)
            .getReference();
  }

  private Branch commit(Branch branch, ContentKey key, IcebergTable table) {
    return rest()
        .body(
            ImmutableOperations.builder()
                .commitMeta(CommitMeta.fromMessage("test commit"))
                .addOperations(Operation.Put.of(key, table))
                .build())
        .post("trees/{ref}/history/commit", branch.toPathString())
        .then()
        .statusCode(200)
        .extract()
        .as(CommitResponse.class)
        .getTargetBranch();
  }

  private Content getContent(Reference reference, ContentKey key) {
    return rest()
        .get("trees/{ref}/contents/{key}", reference.toPathString(), key.toPathString())
        .then()
        .statusCode(200)
        .extract()
        .as(ContentResponse.class)
        .getContent();
  }

  @ParameterizedTest
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
    Branch branch = createBranch(branchName);
    ContentKey key = ContentKey.fromPathString(encodedKey);
    IcebergTable table = IcebergTable.of("test-location", 1, 2, 3, 4);
    branch = commit(branch, key, table);
    assertThat(getContent(branch, key))
        .asInstanceOf(InstanceOfAssertFactories.type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("test-location");
  }

  @ParameterizedTest
  @ValueSource(strings = {"simple", "with/slash"})
  void testGetSeveralContents(String branchName) {
    Branch branch = createBranch(branchName);
    ContentKey key1 = ContentKey.of("test", "Key");
    ContentKey key2 = ContentKey.of("test.with.dot", "Key");
    IcebergTable table1 = IcebergTable.of("loc1", 1, 2, 3, 4);
    IcebergTable table2 = IcebergTable.of("loc2", 1, 2, 3, 4);
    branch = commit(branch, key1, table1);
    branch = commit(branch, key2, table2);

    Stream<MapEntry<ContentKey, String>> entries =
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

    assertThat(entries).containsExactlyInAnyOrder(entry(key1, "loc1"), entry(key2, "loc2"));
  }

  /** Dedicated test for human-readable references in URL paths. */
  @Test
  void testBranchWithSlashInUrlPath(@NessieClientUri URI clientUri) throws IOException {
    Branch branch = createBranch("test/branch/name1");

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
    URL url = new URL(clientUri + "/trees/test/branch/name1@");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    assertThat(conn.getResponseCode()).isEqualTo(200);
    conn.disconnect();
  }

  /** Dedicated test for human-readable default branch references in URL paths. */
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

    Branch branch = createBranch("testDefaultBranchSpecInUrlPath");
    ContentKey key = ContentKey.of("test1");
    commit(branch, key, IcebergTable.of("loc", 1, 2, 3, 4));

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

  @Test
  public void testCommitMetaAttributes() {
    Branch branch = createBranch("testCommitMetaAttributes");
    commit(branch, ContentKey.of("test-key"), IcebergTable.of("meta", 1, 2, 3, 4));

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
}
