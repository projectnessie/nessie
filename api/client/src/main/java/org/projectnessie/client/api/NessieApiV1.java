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
package org.projectnessie.client.api;

import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.RefLogResponse;

/** Interface for the Nessie V1 API implementation. */
public interface NessieApiV1 extends NessieApi {

  /** Get the server configuration. */
  NessieConfiguration getConfig();

  /** Get details for the default reference. */
  Branch getDefaultBranch() throws NessieNotFoundException;

  GetContentBuilder getContent();

  /** Get all references. */
  GetAllReferencesBuilder getAllReferences();

  /** Create a new reference. */
  CreateReferenceBuilder createReference();

  /** Get details of a particular ref, if it exists. */
  GetReferenceBuilder getReference();

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
   */
  GetEntriesBuilder getEntries();

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
   */
  GetCommitLogBuilder getCommitLog();

  /** Update a tag. */
  AssignTagBuilder assignTag();

  /** Delete a tag. */
  DeleteTagBuilder deleteTag();

  /** Update a branch. */
  AssignBranchBuilder assignBranch();

  /** Delete a branch. */
  DeleteBranchBuilder deleteBranch();

  /** cherry pick a set of commits into a branch. */
  TransplantCommitsBuilder transplantCommitsIntoBranch();

  /** merge mergeRef onto ref. */
  MergeReferenceBuilder mergeRefIntoBranch();

  CommitMultipleOperationsBuilder commitMultipleOperations();

  /** Retrieve a diff between two references. */
  GetDiffBuilder getDiff();

  /**
   * Retrieve the reflog from the HEAD of the RefLog resource, potentially truncated by the backend.
   *
   * <p><em>The Nessie reflog in this form is deprecated, likely for removal.</em>
   *
   * <p>Retrieves up to {@code maxRecords} refLog-entries starting at the HEAD of the RefLog
   * resource. The backend <em>may</em> respect the given {@code max} records hint, but return less
   * or more entries. Backends may also cap the returned entries at a hard-coded limit, the default
   * REST server implementation has such a hard-coded limit.
   *
   * <p>Invoking {@code getRefLog()} does <em>not</em> guarantee to return all reflog entries,
   * because the result can be truncated by the backend.
   *
   * <p>To implement paging, check {@link RefLogResponse#isHasMore() RefLogResponse.isHasMore()}
   * and, if {@code true}, pass the value of {@link RefLogResponse#getToken()
   * RefLogResponse.getToken()} in the next invocation of {@code getRefLog()} as the {@code
   * pageToken} parameter.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  GetRefLogBuilder getRefLog();

  /** Fetch a single {@link org.projectnessie.model.Namespace}. */
  GetNamespaceBuilder getNamespace();

  /** Fetch one or more {@link org.projectnessie.model.Namespace}s based on a given prefix. */
  GetMultipleNamespacesBuilder getMultipleNamespaces();

  /** Create a single {@link org.projectnessie.model.Namespace}. */
  CreateNamespaceBuilder createNamespace();

  /** Delete a single {@link org.projectnessie.model.Namespace}. */
  DeleteNamespaceBuilder deleteNamespace();

  /** Updates properties of a {@link org.projectnessie.model.Namespace}. */
  UpdateNamespaceBuilder updateProperties();
}
