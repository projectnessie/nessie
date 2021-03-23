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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;
import org.projectnessie.model.Validation;

public interface TreeApi {
  /**
   * Get all references.
   */
  List<Reference> getAllReferences();

  /**
   * Get details for the default reference.
   */
  Branch getDefaultBranch() throws NessieNotFoundException;

  /**
   * Create a new reference.
   */
  void createReference(
      @Valid
      @NotNull
          Reference reference)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Get details of a particular ref, if it exists.
   */
  Reference getReferenceByName(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
          String refName)
      throws NessieNotFoundException;

  /**
   * Retrieve objects for a ref, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} objects for the
   * given named reference (tag or branch) or the given {@link org.projectnessie.model.Hash}.
   * The backend <em>may</em> respect the given {@code max} records hint, but return less or
   * more entries. Backends may also cap the returned entries at a hard-coded limit, the default
   * REST server implementation has such a hard-coded limit.</p>
   *
   * <p>Invoking {@code getEntries()} does <em>not</em> guarantee to return
   * all commit log entries of a given reference, because the result can be truncated by the
   * backend.</p>
   *
   * <p>To implement paging, check {@link EntriesResponse#hasMore() EntriesResponse.hasMore()} and, if
   * {@code true}, pass the value of {@link EntriesResponse#getToken() EntriesResponse.getToken()}
   * in the next invocation of {@code getEntries()} as the {@code pageToken} parameter.</p>
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.</p>
   */
  EntriesResponse getEntries(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
          String refName,
          Integer maxRecords,
          String pageToken)
          throws NessieNotFoundException;

  /**
   * Retrieve the commit log for a ref, potentially truncated by the backend.
   *
   * <p>Retrieves up to {@code maxRecords} commit-log-entries starting at the HEAD of the
   * given named reference (tag or branch) or the given {@link org.projectnessie.model.Hash}.
   * The backend <em>may</em> respect the given {@code max} records hint, but return less or
   * more entries. Backends may also cap the returned entries at a hard-coded limit, the default
   * REST server implementation has such a hard-coded limit.</p>
   *
   * <p>Invoking {@code getCommitLog()} does <em>not</em> guarantee to return
   * all commit log entries of a given reference, because the result can be truncated by the
   * backend.</p>
   *
   * <p>To implement paging, check {@link LogResponse#hasMore() LogResponse.hasMore()} and, if
   * {@code true}, pass the value of {@link LogResponse#getToken() LogResponse.getToken()}
   * in the next invocation of {@code getCommitLog()} as the {@code pageToken} parameter.</p>
   *
   * <p>See {@code org.projectnessie.client.StreamingUtil} in {@code nessie-client}.</p>
   */
  LogResponse getCommitLog(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
          String ref,
          Integer maxRecords,
          String pageToken)
          throws NessieNotFoundException;

  /**
   * Update a tag.
   */
  void assignTag(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String tagName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String oldHash,
      @Valid
      @NotNull
          Tag tag
      ) throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a tag.
   */
  void deleteTag(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String tagName,
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash
      ) throws NessieConflictException, NessieNotFoundException;

  /**
   * Update a branch.
   */
  void assignBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String oldHash,
      @Valid
      @NotNull
          Branch branch
      ) throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a branch.
   */
  void deleteBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash
      ) throws NessieConflictException, NessieNotFoundException;

  /**
   * cherry pick a set of commits into a branch.
   */
  void transplantCommitsIntoBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
          String message,
      @Valid
          Transplant transplant)
          throws NessieNotFoundException, NessieConflictException;

  /**
   * merge mergeRef onto ref.
   */
  void mergeRefIntoBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
      @Valid
      @NotNull
          Merge merge)
          throws NessieNotFoundException, NessieConflictException;

  void commitMultipleOperations(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
          String message,
      @Valid
      @NotNull
          Operations operations)
      throws NessieNotFoundException, NessieConflictException;
}
