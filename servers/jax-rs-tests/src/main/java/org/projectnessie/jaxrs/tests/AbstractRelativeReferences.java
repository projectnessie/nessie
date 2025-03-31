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

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.error.ErrorCode.BAD_REQUEST;
import static org.projectnessie.jaxrs.tests.BaseTestNessieRest.rest;

import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
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
import org.projectnessie.model.Tag;

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
                           |
                          tag1
    */
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
    outer.createTagV2("tag1", etl2);
    etl2 = outer.commitV2(etl2, key5, table5);
    c5 = etl2.getHash();
  }

  @Test
  public void withTimestamp() {
    var c5commit = metaForCommit("etl2@" + c5);
    var c5timestamp = requireNonNull(c5commit.getCommitTime());
    assertThat(c5commit.getHash()).isEqualTo(c5);

    var c5plus1 = c5timestamp.plus(1, ChronoUnit.SECONDS);

    assertThat(metaForCommit("etl2*" + c5plus1)).isEqualTo(c5commit);

    var c5rounded = c5plus1.minusNanos(c5plus1.getNano());

    assertThat(metaForCommit("etl2*" + c5rounded.plus(1, ChronoUnit.MILLIS))).isEqualTo(c5commit);
    assertThat(metaForCommit("etl2*" + c5rounded.plus(1, ChronoUnit.MICROS))).isEqualTo(c5commit);
    assertThat(metaForCommit("etl2*" + c5rounded.plus(1, ChronoUnit.NANOS))).isEqualTo(c5commit);
    assertThat(metaForCommit("etl2*" + c5rounded)).isEqualTo(c5commit);
  }

  /**
   * Can create a reference from a branch + unambiguous target hash. Expect a 200 response with the
   * created reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromBranchUnambiguous() {
    Reference reference =
        expectBranch(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Branch.of("base", c2 + "~1"))
                .post("trees"));
    assertThat(reference.getName()).isEqualTo("branch1");
    assertThat(reference.getHash()).isEqualTo(c1);
  }

  /**
   * Cannot create a reference from a branch + ambiguous target hash (writing operation). The hash
   * must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromBranchAmbiguous() {
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

  /** Cannot create a reference from a branch + missing target hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromBranchHead() {
    NessieError error =
        expectError(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Branch.of("etl2", null))
                .post("trees"),
            400);
    checkError(error, BAD_REQUEST, "Target hash must be provided.");
  }

  /**
   * Can create a reference from a tag + unambiguous target hash. Expect a 200 response with the
   * created reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromTagUnambiguous() {
    Reference reference =
        expectBranch(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Tag.of("tag1", c4 + "~1"))
                .post("trees"));
    assertThat(reference.getName()).isEqualTo("branch1");
    assertThat(reference.getHash()).isEqualTo(c3);
  }

  /**
   * Cannot create a reference from a tag + ambiguous target hash (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromTagAmbiguous() {
    NessieError error =
        expectError(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Tag.of("tag1", "~1"))
                .post("trees"),
            400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /** Cannot create a reference from a tag + missing target hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromTagHead() {
    NessieError error =
        expectError(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Tag.of("tag1", null))
                .post("trees"),
            400);
    checkError(error, BAD_REQUEST, "Target hash must be provided.");
  }

  /**
   * Can create a reference from a DETACHED + unambiguous target hash. Expect a 200 response with
   * the created reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromDetachedUnambiguous() {
    Reference reference =
        expectBranch(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Detached.of(c2 + "~1"))
                .post("trees"));
    assertThat(reference.getName()).isEqualTo("branch1");
    assertThat(reference.getHash()).isEqualTo(c1);
  }

  /**
   * Cannot create a reference from a DETACHED + ambiguous target hash (writing operation). The hash
   * must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void createReferenceFromDetachedAmbiguous() {
    NessieError error =
        expectError(
            rest()
                .queryParam("name", "branch1")
                .queryParam("type", "branch")
                .body(Detached.of("~1"))
                .post("trees"),
            400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /**
   * Can assign a reference from a branch + unambiguous expected hash, and the hash is the current
   * HEAD of the reference. Expect a 200 response.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromBranchUnambiguous() {
    Reference base = expectBranch(rest().body(etl1).put("trees/base@{hash}", c3 + "~1"));
    outer.soft.assertThat(base.getHash()).isEqualTo(c3);
  }

  /**
   * Cannot assign a reference from a branch + unambiguous expected hash, when the hash is not the
   * current head of the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromBranchUnambiguousNotAtExpectedHash() {
    NessieError error = expectError(rest().body(etl1).put("trees/base@{hash}", c3 + "~2"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'base' is not at expected hash");
  }

  /**
   * Cannot assign a reference from a branch + unambiguous expected hash, when the hash is not
   * reachable from the current head of the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromBranchUnambiguousExpectedHashUnreachable() {
    NessieError error = expectError(rest().body(etl1).put("trees/base@{hash}", c5 + "~1"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'base' is not at expected hash");
  }

  /**
   * Cannot assign a reference from a branch + unambiguous expected hash, when the hash does not
   * exist. Expect a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromBranchUnambiguousExpectedHashNonExistent() {
    NessieError error = expectError(rest().body(etl1).put("trees/base@cafebabe~1"), 404);
    checkError(error, ErrorCode.REFERENCE_NOT_FOUND, "Commit 'cafebabe' not found");
  }

  /**
   * Cannot assign a reference from a branch + ambiguous expected hash (writing operation). The hash
   * must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromBranchAmbiguous() {
    NessieError error = expectError(rest().body(etl1).put("trees/base@{hash}", "^1"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /**
   * Cannot assign a reference from a branch + missing expected hash. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromBranchHead() {
    NessieError error = expectError(rest().body(etl1).put("trees/base"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must be provided.");
  }

  /**
   * Can assign a reference from a tag + unambiguous expected hash, and the hash is the current HEAD
   * of the reference. Expect a 200 response.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromTagUnambiguous() {
    Reference base = expectTag(rest().body(etl1).put("trees/tag1@{hash}", c5 + "~1"));
    outer.soft.assertThat(base.getHash()).isEqualTo(c3);
  }

  /**
   * Cannot assign a reference from a tag + unambiguous expected hash, when the hash is not the
   * current head of the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromTagUnambiguousNotAtExpectedHash() {
    NessieError error = expectError(rest().body(etl1).put("trees/tag1@{hash}", c5 + "~2"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'tag1' is not at expected hash");
  }

  /**
   * Cannot assign a reference from a tag + unambiguous expected hash, when the hash is not
   * reachable from the current head of the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromTagUnambiguousExpectedHashUnreachable() {
    NessieError error =
        expectError(
            rest()
                .body(etl1)
                .put(
                    "trees/tag1@{hash}",
                    c5 + "*" + Instant.now().plusSeconds(60) /* resolves to c5 itself */),
            409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'tag1' is not at expected hash");
  }

  /**
   * Cannot assign a reference from a tag + unambiguous expected hash, when the hash does not exist.
   * Expect a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromTagUnambiguousExpectedHashNonExistent() {
    NessieError error = expectError(rest().body(etl1).put("trees/tag1@cafebabe~1"), 404);
    checkError(error, ErrorCode.REFERENCE_NOT_FOUND, "Commit 'cafebabe' not found");
  }

  /**
   * Cannot assign a reference from a tag + ambiguous expected hash (writing operation). The hash
   * must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromTagAmbiguous() {
    NessieError error = expectError(rest().body(etl1).put("trees/tag1@{hash}", "~1"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /** Cannot assign a reference from a tag + missing expected hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromTagHead() {
    NessieError error = expectError(rest().body(etl1).put("trees/tag1"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must be provided.");
  }

  /**
   * Cannot assign a reference from a DETACHED ref (assign-from ref must be a branch or a tag).
   * Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromDetachedUnambiguous() {
    NessieError error = expectError(rest().body(etl1).put("trees/@{hash}", c2 + "~1"), 400);
    checkError(error, BAD_REQUEST, "Assignment target must be a branch or a tag.");
  }

  /**
   * Cannot assign a reference from a DETACHED ref (assign-from ref must be a branch or a tag).
   * Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceFromDetachedAmbiguous() {
    NessieError error = expectError(rest().body(etl1).put("trees/@{hash}", "~1"), 400);
    checkError(error, BAD_REQUEST, "Assignment target must be a branch or a tag.");
  }

  /**
   * Can assign a reference to a branch + unambiguous target hash. Expect a 200 response with the
   * updated reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToBranchUnambiguous() {
    etl1 =
        expectBranch(
            rest().body(Branch.of("base", c2 + "~1")).put("trees/etl1@{hash}", etl1.getHash()));
    outer.soft.assertThat(etl1.getHash()).isEqualTo(c1);
  }

  /** Cannot assign a reference to a branch + ambiguous target hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToBranchAmbiguous() {
    NessieError error =
        expectError(
            rest().body(Branch.of("etl2", "~1")).put("trees/etl1@{hash}", etl1.getHash()), 400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /** Cannot assign a reference to a branch + missing target hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToBranchHead() {
    NessieError error =
        expectError(
            rest().body(Branch.of("base", null)).put("trees/etl1@{hash}", etl1.getHash()), 400);
    checkError(error, BAD_REQUEST, "Target hash must be provided.");
  }

  /**
   * Can assign a reference to a tag + unambiguous target hash. Expect a 200 response with the
   * updated reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToTagUnambiguous() {
    etl1 =
        expectBranch(
            rest().body(Tag.of("tag1", c4 + "~1")).put("trees/etl1@{hash}", etl1.getHash()));
    outer.soft.assertThat(etl1.getHash()).isEqualTo(c3);
  }

  /**
   * Cannot assign a reference to a tag + ambiguous target hash (writing operation). Expect a
   * BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToTagAmbiguous() {
    NessieError error =
        expectError(
            rest().body(Tag.of("tag1", "~1")).put("trees/etl1@{hash}", etl1.getHash()), 400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /** Cannot assign a reference to a tag + missing target hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToTagHead() {
    NessieError error =
        expectError(
            rest().body(Tag.of("tag1", null)).put("trees/etl1@{hash}", etl1.getHash()), 400);
    checkError(error, BAD_REQUEST, "Target hash must be provided.");
  }

  /**
   * Can assign a reference to a DETACHED + unambiguous target hash. Expect a 200 response with the
   * updated reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToDetachedUnambiguous() {
    etl1 =
        expectBranch(rest().body(Detached.of(c2 + "~1")).put("trees/etl1@{hash}", etl1.getHash()));
    outer.soft.assertThat(etl1.getHash()).isEqualTo(c1);
  }

  /**
   * Cannot assign a reference to a DETACHED + ambiguous target hash (writing operation). The hash
   * must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void assignReferenceToDetachedAmbiguous() {
    NessieError error =
        expectError(rest().body(Detached.of("~1")).put("trees/etl1@{hash}", etl1.getHash()), 400);
    checkError(error, BAD_REQUEST, "Target hash must contain a starting commit ID.");
  }

  /**
   * Can delete a reference at an unambiguous expected hash, and the hash is the current HEAD of the
   * reference. Expect a 200 response with the deleted reference.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceUnambiguous() {
    Reference base = expectBranch(rest().delete("trees/base@{hash}", c3 + "~1"));
    outer.soft.assertThat(base.getHash()).isEqualTo(c2);
  }

  /**
   * Cannot delete a reference at an unambiguous expected hash, when the hash is not the current
   * HEAD of the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceUnambiguousNotAtExpectedHash() {
    NessieError error = expectError(rest().delete("trees/base@{hash}", c3 + "~2"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'base' is not at expected hash");
  }

  /**
   * Cannot delete a reference at an unambiguous expected hash, when the hash is not reachable from
   * the current HEAD of the reference. Expect a REFERENCE_CONFLICT error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceUnambiguousExpectedHashUnreachable() {
    NessieError error = expectError(rest().delete("trees/base@{hash}", c5 + "~1"), 409);
    checkError(
        error, ErrorCode.REFERENCE_CONFLICT, "Named-reference 'base' is not at expected hash");
  }

  /**
   * Cannot delete a reference at an unambiguous expected hash, when the hash does not exist. Expect
   * a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceUnambiguousExpectedHashNonExistent() {
    NessieError error = expectError(rest().delete("trees/base@{hash}", "cafebabe~1"), 404);
    checkError(error, ErrorCode.REFERENCE_NOT_FOUND, "Commit 'cafebabe' not found");
  }

  /**
   * Cannot delete a reference at an ambiguous expected hash (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceAmbiguous() {
    NessieError error = expectError(rest().delete("trees/base@{hash}", "^1"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /** Cannot delete a reference without expected hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void deleteReferenceHead() {
    NessieError error = expectError(rest().delete("trees/base"), 400);
    checkError(error, BAD_REQUEST, "Expected hash must be provided.");
  }

  /**
   * Can commit to a branch + unambiguous expected hash, and the hash is the current HEAD of the
   * reference. Expect a 200 response with the add contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitToBranchUnambiguous() {
    Set<ContentKey> keys =
        outer
            .prepareCommitV2("base@" + c3 + "~1", key3, table3, 2, false)
            .statusCode(200)
            .extract()
            .as(CommitResponse.class)
            .toAddedContentsMap()
            .keySet();
    assertThat(keys).containsOnly(key3);
  }

  /**
   * Can commit to a branch + unambiguous expected hash, even if the hash is not the current HEAD of
   * the reference, since there are no conflicts between the expected hash and HEAD. Expect a 200
   * response with the add contents.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitToBranchUnambiguousNotAtExpectedHash() {
    Set<ContentKey> keys =
        outer
            .prepareCommitV2("base@" + c3 + "~2", key3, table3, 2, false)
            .statusCode(200)
            .extract()
            .as(CommitResponse.class)
            .toAddedContentsMap()
            .keySet();
    assertThat(keys).containsOnly(key3);
  }

  /**
   * Cannot commit to a branch + unambiguous expected hash, when the hash is not reachable from the
   * current HEAD of the reference. Expect a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitToBranchUnambiguousUnreachable() {
    NessieError error =
        expectError(outer.prepareCommitV2("base@" + c5 + "~1", key3, table3, 2, false), 404);
    checkError(
        error,
        ErrorCode.REFERENCE_NOT_FOUND,
        "Could not find commit '" + c4 + "' in reference 'base'");
  }

  /**
   * Cannot commit to a branch + unambiguous expected hash, when the hash does not exist. Expect a
   * REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitToBranchUnambiguousNonExistent() {
    NessieError error =
        expectError(outer.prepareCommitV2("base@cafebabe~1", key3, table3, 2, false), 404);
    checkError(error, ErrorCode.REFERENCE_NOT_FOUND, "Commit 'cafebabe' not found");
  }

  /**
   * Cannot commit to a branch + ambiguous expected hash (writing operation). The hash must contain
   * a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void commitToBranchAmbiguous() {
    NessieError error =
        expectError(
            outer.prepareCommitV2("base*2023-01-01T00:00:00.000Z", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /** Cannot commit to a branch without expected hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitToBranchHead() {
    NessieError error = expectError(outer.prepareCommitV2("base", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Expected hash must be provided.");
  }

  /**
   * Cannot commit to a tag + unambiguous expected hash (target reference must be a branch), even if
   * the hash is unambiguous.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitToTagUnambiguous() {
    NessieError error =
        expectError(outer.prepareCommitV2("tag1@" + c4 + "~1", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Reference to commit into must be a branch.");
  }

  /** Cannot commit to a tag + ambiguous expected hash (target reference must be a branch). */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitToTagAmbiguous() {
    NessieError error = expectError(outer.prepareCommitV2("tag1@~1", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Reference to commit into must be a branch.");
  }

  /** Cannot commit to a tag without expected hash (target reference must be a branch). */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitToTagHead() {
    NessieError error = expectError(outer.prepareCommitV2("tag1", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Reference to commit into must be a branch.");
  }

  /**
   * Cannot commit to a DETACHED ref (target reference must be a branch). Expect a BAD_REQUEST
   * error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitToDetachedUnambiguous() {
    NessieError error =
        expectError(outer.prepareCommitV2("@" + c5 + "~1", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Reference to commit into must be a branch.");
  }

  /**
   * Cannot commit to a DETACHED ref (target reference must be a branch). Expect a BAD_REQUEST
   * error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void commitToDetachedAmbiguous() {
    NessieError error = expectError(outer.prepareCommitV2("@~1", key3, table3, 2, false), 400);
    checkError(error, BAD_REQUEST, "Reference to commit into must be a branch.");
  }

  /**
   * Can merge from a branch + unambiguous source hash. Expect a 200 response with the merge result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeFromBranchUnambiguous() {
    MergeResponse response = expectMerge(outer.prepareMergeV2("base@" + c2, "etl2", c5 + "~2", 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Cannot merge from a reference + ambiguous source hash (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeFromBranchAmbiguous() {
    NessieError error =
        expectError(
            outer.prepareMergeV2("base@" + c2, "etl1", "*2023-01-01T00:00:00.000Z", 2), 400);
    checkError(error, BAD_REQUEST, "Source hash must contain a starting commit ID.");
  }

  /** Cannot merge from a reference without source hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeFromBranchHead() {
    NessieError error = expectError(outer.prepareMergeV2("base@" + c2, "etl1", null, 2), 400);
    checkError(error, BAD_REQUEST, "Source hash must be provided.");
  }

  /**
   * Can merge from a tag + unambiguous source hash. Expect a 200 response with the merge result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeFromTagUnambiguous() {
    MergeResponse response = expectMerge(outer.prepareMergeV2("base@" + c2, "tag1", c5 + "~1", 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Cannot merge from a tag + ambiguous source hash (writing operation). The hash must contain a
   * starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeFromTagAmbiguous() {
    NessieError error = expectError(outer.prepareMergeV2("base@" + c2, "tag1", "~2", 2), 400);
    checkError(error, BAD_REQUEST, "Source hash must contain a starting commit ID.");
  }

  /** Cannot merge from a tag without source hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeFromTagHead() {
    NessieError error = expectError(outer.prepareMergeV2("base@" + c2, "tag1", null, 2), 400);
    checkError(error, BAD_REQUEST, "Source hash must be provided.");
  }

  /**
   * Can merge from a DETACHED ref + unambiguous source hash. Expect a 200 response with the merge
   * result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeFromDetachedUnambiguous() {
    MergeResponse response =
        expectMerge(outer.prepareMergeV2("base@" + c2, "DETACHED", c5 + "~2", 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Cannot merge from a DETACHED ref + ambiguous source hash (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeFromDetachedAmbiguous() {
    NessieError error = expectError(outer.prepareMergeV2("base@" + c2, "DETACHED", "~2", 2), 400);
    checkError(error, BAD_REQUEST, "Source hash must contain a starting commit ID.");
  }

  /**
   * Can merge to a branch + unambiguous expected hash, and the hash is the current HEAD of the
   * reference. Expect a 200 response with the merge result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToBranchUnambiguous() {
    MergeResponse response = expectMerge(outer.prepareMergeV2("base@" + c3 + "~1", "etl1", c3, 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Can merge to a branch + unambiguous expected hash, even if the hash is not the current HEAD of
   * the reference, since for merges and transplants, the expected hash is purely informational.
   * Expect a 200 response with the merge result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToBranchUnambiguousNotAtExpectedHash() {
    MergeResponse response = expectMerge(outer.prepareMergeV2("base@" + c3 + "~2", "etl1", c3, 2));
    checkMerge(response, c2, c2);
  }

  /**
   * Cannot merge to a branch + unambiguous expected hash, when the hash is not reachable from the
   * current HEAD of the reference. Expect a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToBranchUnambiguousExpectedHashUnreachable() {
    NessieError error = expectError(outer.prepareMergeV2("base@" + c5 + "~1", "etl1", c3, 2), 404);
    checkError(
        error,
        ErrorCode.REFERENCE_NOT_FOUND,
        "Could not find commit '" + c4 + "' in reference 'base'");
  }

  /**
   * Cannot merge to a branch + unambiguous expected hash, when the hash does not exist. Expect a
   * REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToBranchUnambiguousExpectedHashNonExistent() {
    NessieError error = expectError(outer.prepareMergeV2("base@cafebabe~1", "etl1", c3, 2), 404);
    checkError(error, ErrorCode.REFERENCE_NOT_FOUND, "Commit 'cafebabe' not found");
  }

  /**
   * Cannot merge to a branch + ambiguous expected hash (writing operation). The hash must contain a
   * starting commit ID. Expect a BAD_REQUEST error.
   *
   * <p>Note: since for merges and transplants, the expected hash is purely informational, ambiguous
   * hashes could in theory be allowed, but this would be confusing to users.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void mergeToBranchAmbiguous() {
    NessieError error =
        expectError(outer.prepareMergeV2("base@*2023-01-01T00:00:00.000Z", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /** Cannot merge to a branch without expected hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeToBranchHead() {
    NessieError error = expectError(outer.prepareMergeV2("base", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Expected hash must be provided.");
  }

  /**
   * Cannot merge to a tag (target reference must be a branch), even with unambiguous expected hash.
   * Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeToTagUnambiguous() {
    NessieError error = expectError(outer.prepareMergeV2("tag1@" + c4 + "~1", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to merge into must be a branch.");
  }

  /** Cannot merge to a tag (target reference must be a branch). Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeToTagAmbiguous() {
    NessieError error = expectError(outer.prepareMergeV2("tag1@~1", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to merge into must be a branch.");
  }

  /**
   * Cannot merge to a tag without expected hash (target reference must be a branch). Expect a
   * BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeToTagHead() {
    NessieError error = expectError(outer.prepareMergeV2("tag1", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to merge into must be a branch.");
  }

  /**
   * Cannot merge to a DETACHED ref (target reference must be a branch), even with unambiguous
   * expected hash. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeToDetachedUnambiguous() {
    NessieError error = expectError(outer.prepareMergeV2("@" + c5 + "~1", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to merge into must be a branch.");
  }

  /**
   * Cannot merge to a DETACHED ref (target reference must be a branch). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void mergeToDetachedAmbiguous() {
    NessieError error =
        expectError(outer.prepareMergeV2("@*2023-01-01T00:00:00.000Z", "etl1", c3, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to merge into must be a branch.");
  }

  /**
   * Can transplant from a reference + unambiguous source hashes. Expect a 200 response with the
   * transplant result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantFromBranchUnambiguous() {
    List<String> hashes =
        Arrays.asList(
            c5 + "~1", // c4
            c5 + "*" + Instant.now().plusSeconds(60)); // c5
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c2, "etl2", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Cannot transplant from a reference + ambiguous source hashes (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantFromBranchAmbiguous() {
    List<String> hashes = Arrays.asList(c5, "~1");
    NessieError error =
        expectError(outer.prepareTransplantV2("base@" + c2, "etl2", hashes, 2), 400);
    outer
        .soft
        .assertThat(error.getMessage())
        .contains("Hash to transplant must contain a starting commit ID.");
  }

  /**
   * Can transplant from a tag + unambiguous source hash. Expect a 200 response with the transplant
   * result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantFromTagUnambiguous() {
    List<String> hashes = Collections.singletonList(c4 + "~1");
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c2, "tag1", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Cannot transplant from a tag + ambiguous source hash (writing operation). The hash must contain
   * a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantFromTagAmbiguous() {
    List<String> hashes = Collections.singletonList("~1");
    NessieError error =
        expectError(outer.prepareTransplantV2("base@" + c2, "tag1", hashes, 2), 400);
    outer
        .soft
        .assertThat(error.getMessage())
        .contains("Hash to transplant must contain a starting commit ID.");
  }

  /**
   * Can transplant from a DETACHED ref + unambiguous source hash. Expect a 200 response with the
   * transplant result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantFromDetachedUnambiguous() {
    List<String> hashes = Collections.singletonList(c5 + "~1");
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c2, "DETACHED", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Cannot transplant from a DETACHED ref + ambiguous source hash (writing operation). The hash
   * must contain a starting commit ID. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantFromDetachedAmbiguous() {
    List<String> hashes = Collections.singletonList("~1");
    NessieError error =
        expectError(outer.prepareTransplantV2("base@" + c2, "DETACHED", hashes, 2), 400);
    outer
        .soft
        .assertThat(error.getMessage())
        .contains("Hash to transplant must contain a starting commit ID.");
  }

  /**
   * Can transplant to a branch + unambiguous expected hash, and the hash is the current HEAD of the
   * reference. Expect a 200 response with the transplant result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToBranchUnambiguous() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c3 + "~1", "etl2", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Can transplant to a branch + unambiguous expected hash, even if the hash is not the current
   * HEAD of the reference, since for merges and transplants, the expected hash is purely
   * informational. Expect a 200 response with the transplant result.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToBranchUnambiguousNotAtExpectedHash() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    MergeResponse response =
        expectMerge(outer.prepareTransplantV2("base@" + c3 + "~2", "etl2", hashes, 2));
    outer.soft.assertThat(response.getEffectiveTargetHash()).isEqualTo(c2);
  }

  /**
   * Cannot transplant to a branch + unambiguous expected hash, when the hash is not reachable from
   * the current HEAD of the reference. Expect a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToBranchUnambiguousExpectedHashUnreachable() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    NessieError error =
        expectError(outer.prepareTransplantV2("base@" + c5 + "~1", "etl2", hashes, 2), 404);
    checkError(
        error,
        ErrorCode.REFERENCE_NOT_FOUND,
        "Could not find commit '" + c4 + "' in reference 'base'");
  }

  /**
   * Cannot transplant to a branch + unambiguous expected hash, when the hash does not exist. Expect
   * a REFERENCE_NOT_FOUND error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToBranchUnambiguousExpectedHashNonExistent() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    NessieError error =
        expectError(outer.prepareTransplantV2("base@cafebabe~1", "etl2", hashes, 2), 404);
    checkError(error, ErrorCode.REFERENCE_NOT_FOUND, "Commit 'cafebabe' not found");
  }

  /**
   * Cannot transplant to a branch + ambiguous expected hash (writing operation). The hash must
   * contain a starting commit ID. Expect a BAD_REQUEST error.
   *
   * <p>Note: since for merges and transplants, the expected hash is purely informational, ambiguous
   * hashes could in theory be allowed, but this would be confusing to users.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void transplantToBranchAmbiguous() {
    List<String> hashes = Arrays.asList(c5, c4);
    NessieError error =
        expectError(
            outer.prepareTransplantV2("base@*2023-01-01T00:00:00.000Z", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Expected hash must contain a starting commit ID.");
  }

  /** Cannot transplant to a branch without expected hash. Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantToBranchHead() {
    List<String> hashes = Arrays.asList(c5, c4);
    NessieError error = expectError(outer.prepareTransplantV2("base", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Expected hash must be provided.");
  }

  /**
   * Cannot transplant to a tag (target reference must be a branch), even with unambiguous expected
   * hash. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantToTagUnambiguous() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    NessieError error =
        expectError(outer.prepareTransplantV2("tag1@" + c4 + "~1", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to transplant into must be a branch.");
  }

  /** Cannot transplant to a tag (target reference must be a branch). Expect a BAD_REQUEST error. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantToTagAmbiguous() {
    List<String> hashes = Arrays.asList(c5, c4);
    NessieError error = expectError(outer.prepareTransplantV2("tag1@~1", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to transplant into must be a branch.");
  }

  /** Cannot transplant to a tag without expected hash (target reference must be a branch). */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantToTagHead() {
    List<String> hashes = Arrays.asList(c5, c4);
    NessieError error = expectError(outer.prepareTransplantV2("tag1", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to transplant into must be a branch.");
  }

  /**
   * Cannot transplant to a DETACHED ref (target reference must be a branch), even with unambiguous
   * expected hash. Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantToDetachedUnambiguous() {
    List<String> hashes = Arrays.asList(c3, c4, c5);
    NessieError error =
        expectError(outer.prepareTransplantV2("@" + c5 + "~1", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to transplant into must be a branch.");
  }

  /**
   * Cannot transplant to a DETACHED ref (target reference must be a branch). Expect a BAD_REQUEST
   * error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  public void transplantToDetachedAmbiguous() {
    List<String> hashes = Arrays.asList(c5, c4);
    NessieError error =
        expectError(
            outer.prepareTransplantV2("@*2023-01-01T00:00:00.000Z", "etl2", hashes, 2), 400);
    checkError(error, BAD_REQUEST, "Reference to transplant into must be a branch.");
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

  /** Can get the commit log with an oldest unambiguous hash. Expect a 200 response with the log. */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogOldestHashUnambiguous() {
    Stream<String> hashes =
        expectCommitLog(rest().queryParam("limit-hash", c4 + "~1").get("trees/etl2/history"));
    assertThat(hashes).containsExactly(c5, c4, c3);
  }

  /**
   * Can get the commit log with an oldest unambiguous hash on a DETACHED ref. Expect a 200 response
   * with the log.
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
   * Can get the commit log with an oldest ambiguous hash (reading operation). Expect a 200 response
   * with the log.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getCommitLogOldestHashAmbiguous() {
    Stream<String> hashes =
        expectCommitLog(rest().queryParam("limit-hash", "~2").get("trees/etl2/history"));
    assertThat(hashes).containsExactly(c5, c4, c3);
  }

  /**
   * Cannot get the commit log with an oldest ambiguous hash on a DETACHED ref (DETACHED requires
   * unambiguous). Expect a BAD_REQUEST error.
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
    checkError(error, BAD_REQUEST, "\"From\" hash must contain a starting commit ID.");
  }

  /**
   * Cannot get a diff between two references that are ambiguous on a DETACHED ref (DETACHED
   * requires unambiguous). Expect a BAD_REQUEST error.
   */
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void getDiffToRefAmbiguousDetached() {
    NessieError error = expectError(rest().get("trees/base@{from}/diff/@{to}", "~1", "~2"), 400);
    checkError(error, BAD_REQUEST, "\"To\" hash must contain a starting commit ID.");
  }

  static CommitMeta metaForCommit(String commitSpec) {
    var log =
        rest()
            .get("trees/{commitSpec}/history?maxRecords=1", commitSpec)
            .as(LogResponse.class)
            .getLogEntries();
    assertThat(log).describedAs("commit spec '%s' yields no result", commitSpec).isNotEmpty();
    return log.get(0).getCommitMeta();
  }

  private static Branch expectBranch(Response base) {
    return (Branch)
        base.then().statusCode(200).extract().as(SingleReferenceResponse.class).getReference();
  }

  private static Tag expectTag(Response base) {
    return (Tag)
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
