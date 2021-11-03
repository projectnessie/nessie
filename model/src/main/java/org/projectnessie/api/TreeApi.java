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
package org.projectnessie.api;

import java.util.List;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Transplant;
import org.projectnessie.model.Validation;

public interface TreeApi {

  // Note: When substantial changes in Nessie API (this and related interfaces) are made
  // the API version number reported by NessieConfiguration.getMaxSupportedApiVersion()
  // should be increased as well.

  /** Get all references. */
  List<Reference> getAllReferences();

  /** Get details for the default reference. */
  Branch getDefaultBranch() throws NessieNotFoundException;

  /**
   * Create a new reference.
   *
   * <p>The type of {@code reference}, which can be either a {@link Branch} or {@link
   * org.projectnessie.model.Tag}, determines the type of the reference to be created.
   *
   * <p>{@link Reference#getName()} defines the the name of the reference to be created, {@link
   * Reference#getHash()} is the hash of the created reference, the HEAD of the created reference.
   * {@code sourceRefName} is the name of the reference which contains {@link Reference#getHash()},
   * and must be present if {@link Reference#getHash()} is present.
   *
   * <p>Specifying no {@link Reference#getHash()} means that the new reference will be created "at
   * the beginning of time".
   */
  Reference createReference(
      @Valid
          @Nullable
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String sourceRefName,
      @Valid @NotNull Reference reference)
      throws NessieNotFoundException, NessieConflictException;

  /** Get details of a particular ref, if it exists. */
  Reference getReferenceByName(
      @Valid
          @NotNull
          @Pattern(
              regexp = Validation.REF_NAME_OR_HASH_REGEX,
              message = Validation.REF_NAME_OR_HASH_MESSAGE)
          String refName)
      throws NessieNotFoundException;

  /**
   * Retrieve objects for a ref, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} objects for the given named reference (tag or branch).
   * The backend <em>may</em> respect the given {@code max} records hint, but return less or more
   * entries. Backends may also cap the returned entries at a hard-coded limit, the default REST
   * server implementation has such a hard-coded limit.
   *
   * <p>Invoking {@code getEntries()} does <em>not</em> guarantee to return all commit log entries
   * of a given reference, because the result can be truncated by the backend.
   *
   * <p>To implement paging, check {@link EntriesResponse#isHasMore() EntriesResponse.isHasMore()}
   * and, if {@code true}, pass the value of {@link EntriesResponse#getToken()
   * EntriesResponse.getToken()} in the next invocation of {@code getEntries()} as the {@code
   * pageToken} parameter.
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.
   */
  EntriesResponse getEntries(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String refName,
      @Valid @NotNull EntriesParams params)
      throws NessieNotFoundException;

  /**
   * Retrieve the commit log for a ref, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} commit-log-entries starting at the HEAD of the given
   * named reference (tag or branch). The backend <em>may</em> respect the given {@code max} records
   * hint, but return less or more entries. Backends may also cap the returned entries at a
   * hard-coded limit, the default REST server implementation has such a hard-coded limit.
   *
   * <p>Invoking {@code getCommitLog()} does <em>not</em> guarantee to return all commit log entries
   * of a given reference, because the result can be truncated by the backend.
   *
   * <p>To implement paging, check {@link LogResponse#isHasMore() LogResponse.isHasMore()} and, if
   * {@code true}, pass the value of {@link LogResponse#getToken() LogResponse.getToken()} in the
   * next invocation of {@code getCommitLog()} as the {@code pageToken} parameter.
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.
   */
  LogResponse getCommitLog(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String ref,
      @Valid @NotNull CommitLogParams params)
      throws NessieNotFoundException;

  /** Update a tag. */
  void assignTag(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String tagName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String oldHash,
      @Valid @NotNull Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  /** Delete a tag. */
  void deleteTag(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String tagName,
      @Valid @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash)
      throws NessieConflictException, NessieNotFoundException;

  /** Update a branch. */
  void assignBranch(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String oldHash,
      @Valid @NotNull Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  /** Delete a branch. */
  void deleteBranch(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash)
      throws NessieConflictException, NessieNotFoundException;

  /** cherry pick a set of commits into a branch. */
  void transplantCommitsIntoBranch(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
      @Valid String message,
      @Valid Transplant transplant)
      throws NessieNotFoundException, NessieConflictException;

  /** merge mergeRef onto ref. */
  void mergeRefIntoBranch(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
      @Valid @NotNull Merge merge)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Commit multiple operations against the given branch expecting that branch to have the given
   * hash as its latest commit. The hash in the successful response contains the hash of the commit
   * that contains the operations of the invocation.
   *
   * @param branchName Branch to change, defaults to default branch.
   * @param hash Expected hash of branch.
   * @param operations {@link Operations} to apply
   * @return updated {@link Branch} objects with the hash of the new HEAD
   * @throws NessieNotFoundException if {@code branchName} could not be found
   * @throws NessieConflictException if the operations could not be applied to some conflict, which
   *     is either caused by a conflicting commit or concurrent commits.
   */
  Branch commitMultipleOperations(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
      @Valid @NotNull Operations operations)
      throws NessieNotFoundException, NessieConflictException;
}
