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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.projectnessie.error.ErrorCode.BAD_REQUEST;
import static org.projectnessie.jaxrs.tests.BaseTestNessieRest.rest;

import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieError;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SingleReferenceResponse;

/** A set of tests specifically aimed at testing relative references in API v2. */
public abstract class AbstractRelativeReferences {

  private final BaseTestNessieRest outer;

  private final ContentKey key1 = ContentKey.of("key1");
  private final ContentKey key2 = ContentKey.of("key2");
  private final ContentKey key3 = ContentKey.of("key3");
  private final ContentKey key4 = ContentKey.of("key4");
  private final ContentKey key5 = ContentKey.of("key5");

  private final IcebergTable table1 = IcebergTable.of("1", 1, 1, 1, 1);
  private final IcebergTable table2 = IcebergTable.of("2", 2, 2, 2, 2);
  private final IcebergTable table3 = IcebergTable.of("3", 3, 3, 3, 3);
  private final IcebergTable table4 = IcebergTable.of("4", 4, 4, 4, 4);
  private final IcebergTable table5 = IcebergTable.of("5", 5, 5, 5, 5);

  private String c1;
  private String c2;
  private String c3;
  private String c4;
  private String c5;

  private Branch etl1;

  protected AbstractRelativeReferences(BaseTestNessieRest outer) {
    this.outer = outer;
  }

  @BeforeEach
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void createFixtures() {
    /*
         base c1 -- c2
                      \
         etl1          c3
                         \
         etl2             c4 -- c5
    */
    assumeTrue(outer.isNewModel());
    Branch base = outer.createBranchV2("base");
    base = outer.commitV2(base, key1, table1);
    c1 = base.getHash();
    base = outer.commitV2(base, key2, table2);
    c2 = base.getHash();
    etl1 = outer.createBranchV2("etl1", base);
    etl1 = outer.commitV2(etl1, key3, table3);
    c3 = etl1.getHash();
    Branch etl2 = outer.createBranchV2("etl2", etl1);
    etl2 = outer.commitV2(etl2, key4, table4);
    c4 = etl2.getHash();
    etl2 = outer.commitV2(etl2, key5, table5);
    c5 = etl2.getHash();
  }

  /**
   * Can create a reference from a hash that is unambiguous. Expect a 200 response with the created
   * reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceUnambiguous() {
    Reference reference =
        expectReference(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Branch.of("base", c2 + "~1"))
                .post("trees"));
    assertThat(reference.getName()).isEqualTo("branch1");
    assertThat(reference.getHash()).isEqualTo(c1);
  }

  /**
   * Can create a reference from a hash that is unambiguous and DETACHED. Expect a 200 response with
   * the created reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceUnambiguousDetached() {
    Reference reference =
        expectReference(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Detached.of(c2 + "~1"))
                .post("trees"));
    assertThat(reference.getName()).isEqualTo("branch1");
    assertThat(reference.getHash()).isEqualTo(c1);
  }

  /**
   * Cannot create a reference from a hash that is ambiguous (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceAmbiguous() {
    NessieError error =
        expectError(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Branch.of("base", "~1"))
                .post("trees"),
            400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /**
   * Can assign a reference from a hash that is unambiguous, but the hash is not the current hash of
   * the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromUnambiguousConflict() {
    NessieError error = expectError(rest().body(etl1).put("trees/base@{hash}", c2 + "~1"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'base' is not at expected hash");
  }

  /**
   * Can assign a reference from a hash that is unambiguous, and the hash is the current HEAD of the
   * reference. Expect a 200 response.
   *
   * <p>This is a convoluted case as the only way to use a relative reference that resolves to the
   * HEAD of a branch is to use a timestamp in the future that resolves to the HEAD itself.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromUnambiguousSuccess() {
    Reference base =
        expectReference(
            rest().body(etl1).put("trees/base@{hash}", c2 + "*" + Instant.now().plusSeconds(60)));
    outer.soft.assertThat(base.getHash()).isEqualTo(c3);
  }

  /**
   * Cannot assign a reference from a hash that is ambiguous (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromAmbiguous() {
    NessieError error = expectError(rest().body(etl1).put("trees/base@{hash}", "^1"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Cannot assign a reference from a DETACHED hash that is ambiguous (DETACHED requires
   * unambiguous). The hash must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromAmbiguousDetached() {
    NessieError error = expectError(rest().body(etl1).put("trees/@~1"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can assign a reference to a hash that is unambiguous. Expect a 200 response with the updated
   * reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToUnambiguous() {
    etl1 =
        expectReference(
            rest().body(Branch.of("base", c2 + "~1")).put("trees/etl1@{hash}", etl1.getHash()));
    outer.soft.assertThat(etl1.getHash()).isEqualTo(c1);
  }

  /**
   * Can assign a reference to a hash that is unambiguous adn DETACHED. Expect a 200 response with
   * the updated reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToUnambiguousDetached() {
    etl1 =
        expectReference(
            rest().body(Detached.of(c2 + "~1")).put("trees/etl1@{hash}", etl1.getHash()));
    outer.soft.assertThat(etl1.getHash()).isEqualTo(c1);
  }

  /**
   * Cannot assign a reference to a hash that is ambiguous (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToAmbiguous() {
    NessieError error =
        expectError(
            rest()
                .body(Branch.of("base", "*2023-01-01T00:00:00.000Z"))
                .put("trees/etl1@{hash}", etl1.getHash()),
            400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /**
   * Can delete a reference that is unambiguous, but the hash is not the current HEAD of the
   * reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceUnambiguousConflict() {
    NessieError error = expectError(rest().delete("trees/base@{hash}", c2 + "~1"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'base' is not at expected hash");
  }

  /**
   * Can delete a reference that is unambiguous, and the hash is the current HEAD of the reference.
   * Expect a 200 response with the deleted reference.
   *
   * <p>This is a convoluted case as the only way to use a relative reference that resolves to the
   * HEAD of a branch is to use a timestamp in the future that resolves to the HEAD itself.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceUnambiguousSuccess() {
    Reference base =
        expectReference(
            rest().delete("trees/base@{hash}", c2 + "*" + Instant.now().plusSeconds(60)));
    outer.soft.assertThat(base.getHash()).isEqualTo(c2);
  }

  /**
   * Can commit to a reference that is unambiguous, even if the hash is not the current HEAD of the
   * reference, since there are no conflicts between the expected hash and HEAD. Expect a 200
   * response with the add contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitUnambiguous() {
    Set<ContentKey> keys =
        outer
            .prepareCommitV2("base@" + c2 + "~1", key3, table3, 2, false)
            .statusCode(200)
            .extract()
            .as(CommitResponse.class)
            .toAddedContentsMap()
            .keySet();
    assertThat(keys).containsOnly(key3);
  }

  /**
   * Cannot commit to a reference that is ambiguous (writing operation). The hash must contain a
   * starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitAmbiguous() {
    NessieError error =
        expectError(
            outer.prepareCommitV2("base*2023-01-01T00:00:00.000Z", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can merge from a reference that is unambiguous. Expect a 200 response with the merge result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeFromUnambiguous() {
    MergeResponse response = expectMerge(outer.prepareMergeV2("base@" + c2, "etl2", c5 + "~2", 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Cannot merge from a reference that is ambiguous (writing operation). The hash must contain a
   * starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeFromAmbiguous() {
    NessieError error =
        expectError(
            outer.prepareMergeV2("base@" + c2, "etl1", "*2023-01-01T00:00:00.000Z", 2), 400);
    checkError(error, BAD_REQUEST, "Source hash must contain a starting commit ID.");
  }

  /**
   * Can merge to a reference that is unambiguous, even if the hash is not the current HEAD of the
   * reference, since for merges and transplants, the expected hash is purely informational. Expect
   * a 200 response with the merge result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToUnambiguous() {
    MergeResponse response = expectMerge(outer.prepareMergeV2("base@" + c2 + "~1", "etl1", c3, 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Cannot merge to a reference that is ambiguous (writing operation). The hash must contain a
   * starting commit ID. Expect a BAD_REQUEST error.
   *
   * <p>Note: since for merges and transplants, the expected hash is purely informational, ambiguous
   * hashes could in theory be allowed, but this would be confusing to users.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToAmbiguous() {
    NessieError error =
        expectError(outer.prepareMergeV2("base@*2023-01-01T00:00:00.000Z", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can transplant from a reference that is unambiguous. Expect a 200 response with the transplant
   * result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantFromUnambiguous() {
    List<String> hashes =
        Arrays.asList(
            c5 + "~1", // c4
            c5 + "*" + Instant.now().plusSeconds(60)); // c5
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c2, "etl2", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Cannot transplant from a reference that is ambiguous (writing operation). The hash must contain
   * a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantFromAmbiguous() {
    List<String> hashes = Arrays.asList(c5, "~1");
    NessieError error =
        expectError(outer.prepareTransplantV2("base@" + c2, "etl2", hashes, 2), 400);
    outer
        .soft
        .assertThat(error.getMessage())
        .contains("Hash to transplant must contain a starting commit ID.");
  }

  /**
   * Can transplant to a reference that is unambiguous, even if the hash is not the current HEAD of
   * the reference, since for merges and transplants, the expected hash is purely informational.
   * Expect a 200 response with the transplant result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToUnambiguous() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c2 + "~1", "etl2", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Cannot transplant to a reference that is ambiguous (writing operation). The hash must contain a
   * starting commit ID. Expect a BAD_REQUEST error.
   *
   * <p>Note: since for merges and transplants, the expected hash is purely informational, ambiguous
   * hashes could in theory be allowed, but this would be confusing to users.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToAmbiguous() {
    List<String> hashes = Arrays.asList(c5, c4);
    NessieError error =
        expectError(
            outer.prepareTransplantV2("base@*2023-01-01T00:00:00.000Z", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can get contents for a reference that is unambiguous. Expect a 200 response with the contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getSeveralContentsUnambiguous() {
    Stream<ContentKey> keys =
        expectContents(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/base@{hash}/contents", c2 + "~1"));
    assertThat(keys).containsOnly(key1);
  }

  /**
   * Can get contents for a DETACHED reference that is unambiguous. Expect a 200 response with the
   * contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getSeveralContentsUnambiguousDetached() {
    Stream<ContentKey> keys =
        expectContents(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/@{hash}/contents", c3 + "~1"));
    assertThat(keys).containsOnly(key1, key2);
  }

  /**
   * Can get contents for a reference that is ambiguous (reading operation). Expect a 200 response
   * with the contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getSeveralContentsAmbiguous() {
    Stream<ContentKey> keys =
        expectContents(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/base@{hash}/contents", "~1"));
    assertThat(keys).containsOnly(key1);
  }

  /**
   * Cannot get contents for a DETACHED reference that is ambiguous (DETACHED requires unambiguous).
   * Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getSeveralContentsAmbiguousDetached() {
    NessieError error =
        expectError(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/@{hash}/contents", "~1"),
            400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can get contents for a reference that is unambiguous. Expect a 200 response with the contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getMultipleContentsUnambiguous() {
    Stream<ContentKey> keys =
        expectContents(
            rest()
                .body(GetMultipleContentsRequest.of(key1, key2))
                .post("trees/base@{hash}/contents", c2 + "~1"));
    assertThat(keys).containsOnly(key1);
  }

  /**
   * Can get contents for a DETACHED reference that is unambiguous. Expect a 200 response with the
   * contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getMultipleContentsUnambiguousDetached() {
    Stream<ContentKey> keys =
        expectContents(
            rest()
                .body(GetMultipleContentsRequest.of(key1, key2))
                .post("trees/@{hash}/contents", c3 + "~1"));
    assertThat(keys).containsOnly(key1, key2);
  }

  /**
   * Can get contents for a reference that is ambiguous (reading operation). Expect a 200 response
   * with the contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getMultipleContentsAmbiguous() {
    Stream<ContentKey> keys =
        expectContents(
            rest()
                .body(GetMultipleContentsRequest.of(key1, key2))
                .post("trees/base@{hash}/contents", "~1"));
    assertThat(keys).containsOnly(key1);
  }

  /**
   * Cannot get contents for a DETACHED reference that is ambiguous (DETACHED requires unambiguous).
   * Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getMultipleContentsAmbiguousDetached() {
    NessieError error =
        expectError(
            rest()
                .body(GetMultipleContentsRequest.of(key1, key2))
                .post("trees/@{hash}/contents", "~1"),
            400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can get the content for a reference that is unambiguous. Expect a 200 response with the
   * content.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getContentUnambiguous() {
    IcebergTable content =
        expectContent(
            rest().get("trees/base@{hash}/contents/{key}", c2 + "~1", key1.toPathString()));
    assertThat(content.getSpecId()).isEqualTo(table1.getSpecId());
  }

  /**
   * Can get the content for a DETACHED reference that is unambiguous. Expect a 200 response with
   * the content.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getContentUnambiguousDetached() {
    IcebergTable content =
        expectContent(rest().get("trees/@{hash}/contents/{key}", c3 + "~1", key2.toPathString()));
    assertThat(content.getSpecId()).isEqualTo(table2.getSpecId());
  }

  /**
   * Can get the content for a reference that is ambiguous (reading operation). Expect a 200
   * response with the content.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getContentAmbiguous() {
    IcebergTable content =
        expectContent(rest().get("trees/base@{hash}/contents/{key}", "~1", key1.toPathString()));
    assertThat(content.getSpecId()).isEqualTo(table1.getSpecId());
  }

  /**
   * Cannot get the content for a DETACHED reference that is ambiguous (DETACHED requires
   * unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getContentAmbiguousDetached() {
    NessieError error =
        expectError(rest().get("trees/@{hash}/contents/{key}", "~1", key1.toPathString()), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can get the entries for a reference that is unambiguous. Expect a 200 response with the
   * entries.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getEntriesUnambiguous() {
    Stream<ContentKey> keys =
        expectEntries(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/base@{hash}/entries", c2 + "~1"));
    assertThat(keys).containsOnly(key1);
  }

  /**
   * Can get the entries for a DETACHED reference that is unambiguous. Expect a 200 response with
   * the entries.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getEntriesUnambiguousDetached() {
    Stream<ContentKey> keys =
        expectEntries(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/@{hash}/entries", c3 + "~1"));
    assertThat(keys).containsOnly(key1, key2);
  }

  /**
   * Can get the entries for a reference that is ambiguous (reading operation). Expect a 200
   * response with the entries.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getEntriesAmbiguous() {
    Stream<ContentKey> keys =
        expectEntries(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/base@{hash}/entries", "~1"));
    assertThat(keys).containsOnly(key1);
  }

  /**
   * Cannot get the entries for a DETACHED reference that is ambiguous (DETACHED requires
   * unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getEntriesAmbiguousDetached() {
    NessieError error =
        expectError(
            rest()
                .queryParam("key", key1.toPathString(), key2.toPathString())
                .get("trees/@{hash}/entries", "~1"),
            400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Can get the commit log for a reference that is unambiguous. Expect a 200 response with the log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogYoungestHashUnambiguous() {
    Stream<String> hashes = expectCommitLog(rest().get("trees/base@{hash}/history", c2 + "~1"));
    assertThat(hashes).containsExactly(c1);
  }

  /**
   * Can get the commit log for a DETACHED reference that is unambiguous. Expect a 200 response with
   * the log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogYoungestHashUnambiguousDetached() {
    Stream<String> hashes = expectCommitLog(rest().get("trees/@{hash}/history", c3 + "~1"));
    assertThat(hashes).containsExactly(c2, c1);
  }

  /**
   * Can get the commit log for a reference that is ambiguous (reading operation). Expect a 200
   * response with the log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogYoungestHashAmbiguous() {
    Stream<String> hashes = expectCommitLog(rest().get("trees/base@{hash}/history", "~1"));
    assertThat(hashes).containsExactly(c1);
  }

  /**
   * Cannot get the commit log for a DETACHED reference that is ambiguous (DETACHED requires
   * unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogYoungestHashAmbiguousDetached() {
    NessieError error = expectError(rest().get("trees/@{hash}/history", "~1"), 400);
    checkError(error, BAD_REQUEST, "Youngest hash must contain a starting commit ID.");
  }

  /**
   * Can get the commit log with an oldest hash that is unambiguous. Expect a 200 response with the
   * log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogOldestHashUnambiguous() {
    Stream<String> hashes =
        expectCommitLog(rest().queryParam("limit-hash", c4 + "~1").get("trees/etl2/history"));
    assertThat(hashes).containsExactly(c5, c4, c3);
  }

  /**
   * Can get the commit log with an oldest hash that is unambiguous on a DETACHED ref. Expect a 200
   * response with the log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogOldestHashUnambiguousDetached() {
    Stream<String> hashes =
        expectCommitLog(
            rest().queryParam("limit-hash", c4 + "~1").get("trees/@{hash}/history", c5));
    assertThat(hashes).containsExactly(c5, c4, c3);
  }

  /**
   * Can get the commit log with an oldest hash that is ambiguous (reading operation). Expect a 200
   * response with the log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogOldestHashAmbiguous() {
    Stream<String> hashes =
        expectCommitLog(rest().queryParam("limit-hash", "~2").get("trees/etl2/history"));
    assertThat(hashes).containsExactly(c5, c4, c3);
  }

  /**
   * Cannot get the commit log with an oldest hash that is ambiguous on a DETACHED ref (DETACHED
   * requires unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogOldestHashAmbiguousDetached() {
    NessieError error =
        expectError(rest().queryParam("limit-hash", "~2").get("trees/@{hash}/history", c5), 400);
    checkError(error, BAD_REQUEST, "Oldest hash must contain a starting commit ID.");
  }

  /**
   * Can get a diff between two references that are unambiguous. Expect a 200 response with the
   * diff.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getDiffFromRefToRefUnambiguous() {
    Stream<ContentKey> keys =
        expectDiff(rest().get("trees/etl1@{from}/diff/base@{to}", c3 + "~1", c2 + "~2"));
    assertThat(keys).containsOnly(key1, key2);
  }

  /**
   * Can get a diff between two references that are ambiguous (reading operation). Expect a 200
   * response with the diff.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getDiffFromRefToRefAmbiguous() {
    Stream<ContentKey> keys =
        expectDiff(rest().get("trees/etl1@{from}/diff/base@{to}", "~1", "~2"));
    assertThat(keys).containsOnly(key1, key2);
  }

  /**
   * Can get a diff between two DETACHED references that are unambiguous. Expect a 200 response with
   * the diff.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getDiffFromRefToRefUnambiguousDetached() {
    Stream<ContentKey> keys =
        expectDiff(rest().get("trees/@{from}/diff/@{to}", c3 + "~1", c2 + "~2"));
    assertThat(keys).containsOnly(key1, key2);
  }

  /**
   * Cannot get a diff between two references that are ambiguous on a DETACHED ref (DETACHED
   * requires unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getDiffFromRefAmbiguousDetached() {
    NessieError error = expectError(rest().get("trees/@{from}/diff/base@{to}", "~1", "~2"), 400);
    checkError(error, BAD_REQUEST, "From hash must contain a starting commit ID.");
  }

  /**
   * Cannot get a diff between two references that are ambiguous on a DETACHED ref (DETACHED
   * requires unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getDiffToRefAmbiguousDetached() {
    NessieError error = expectError(rest().get("trees/base@{from}/diff/@{to}", "~1", "~2"), 400);
    checkError(error, BAD_REQUEST, "To hash must contain a starting commit ID.");
  }

  private static Branch expectReference(Response base) {
    return (Branch)
        base.then().statusCode(200).extract().as(SingleReferenceResponse.class).getReference();
  }

  private static MergeResponse expectMerge(ValidatableResponse etl2) {
    return etl2.statusCode(200).extract().as(MergeResponse.class);
  }

  private static Stream<ContentKey> expectContents(Response key) {
    return key
        .then()
        .statusCode(200)
        .extract()
        .as(GetMultipleContentsResponse.class)
        .getContents()
        .stream()
        .map(GetMultipleContentsResponse.ContentWithKey::getKey);
  }

  private static IcebergTable expectContent(Response response) {
    return (IcebergTable)
        response.then().statusCode(200).extract().as(ContentResponse.class).getContent();
  }

  private static Stream<ContentKey> expectEntries(Response key) {
    return key.then().statusCode(200).extract().as(EntriesResponse.class).getEntries().stream()
        .map(EntriesResponse.Entry::getName);
  }

  private static Stream<String> expectCommitLog(Response c2) {
    return c2.then().statusCode(200).extract().as(LogResponse.class).getLogEntries().stream()
        .map(LogResponse.LogEntry::getCommitMeta)
        .map(CommitMeta::getHash);
  }

  private static Stream<ContentKey> expectDiff(Response response) {
    return response.then().statusCode(200).extract().as(DiffResponse.class).getDiffs().stream()
        .map(DiffResponse.DiffEntry::getKey);
  }

  private static NessieError expectError(Response response, int expectedStatusCode) {
    return expectError(response.then(), expectedStatusCode);
  }

  private static NessieError expectError(ValidatableResponse response, int expectedStatusCode) {
    return response.statusCode(expectedStatusCode).extract().as(NessieError.class);
  }

  private void checkMerge(
      MergeResponse response, String commonAncestor, String effectiveTargetHash) {
    outer.soft.assertThat(response.getCommonAncestor()).isEqualTo(commonAncestor);
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(effectiveTargetHash);
  }

  private void checkError(NessieError error, ErrorCode code, String msg) {
    outer.soft.assertThat(error.getErrorCode()).isEqualTo(code);
    outer.soft.assertThat(error.getMessage()).contains(msg);
  }
}
