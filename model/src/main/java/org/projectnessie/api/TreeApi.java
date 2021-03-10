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
import java.util.OptionalInt;
import java.util.stream.Stream;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;
import org.projectnessie.model.Validation;

@Consumes(value = MediaType.APPLICATION_JSON)
@Path("trees")
public interface TreeApi {
  /**
   * Get all references.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get all references")
  @APIResponses({@APIResponse(responseCode = "200", description = "Returned references.")})
  List<Reference> getAllReferences();

  /**
   * Get details for the default reference.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree")
  @Operation(summary = "Get default branch for commits and reads")
  @APIResponses({
      @APIResponse(responseCode = "200", description = "Found and default bracnh."),
      @APIResponse(responseCode = "404", description = "Default branch not found.")
  })
  Branch getDefaultBranch() throws NessieNotFoundException;

  /**
   * Create a new reference.
   */
  @POST
  @Path("tree")
  @Operation(summary = "Create a new reference")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Created successfully."),
      @APIResponse(responseCode = "409", description = "Reference already exists")
  })
  void createReference(
      @Valid
      @NotNull
      @RequestBody(description = "Reference to create.")
          Reference reference)
      throws NessieNotFoundException, NessieConflictException;

  /**
   * Get details of a particular ref, if it exists.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/{ref}")
  @Operation(summary = "Fetch details of a reference")
  @APIResponses({
      @APIResponse(responseCode = "200", description = "Found and returned reference."),
      @APIResponse(responseCode = "404", description = "Ref not found")
  })
  Reference getReferenceByName(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
      @Parameter(description = "name of ref to fetch")
      @PathParam("ref")
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
   * @see #getEntriesStream(String, OptionalInt)
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/{ref}/entries")
  @Operation(summary = "Fetch all entries for a given reference", description =
      "Retrieves objects for a ref, potentially truncated by the backend.\n"
          + "\n"
          + "Retrieves up to 'maxRecords' entries for the "
          + "given named reference (tag or branch) or the given hash. "
          + "The backend may respect the given 'max' records hint, but return less or more entries. "
          + "Backends may also cap the returned entries at a hard-coded limit, the default "
          + "REST server implementation has such a hard-coded limit.\n"
          + "\n"
          + "To implement paging, check 'hasMore' in the response and, if 'true', pass the value "
          + "returned as 'token' in the next invocation as the 'pageToken' parameter.\n"
          + "\n"
          + "The content and meaning of the returned 'token' is \"private\" to the implementation,"
          + "treat is as an opaque value.\n"
          + "\n"
          + "It is wrong to assume that invoking this method with a very high 'maxRecords' value "
          + "will return all commit log entries.")
  @APIResponses({
      @APIResponse(description = "all objects for a reference"),
      @APIResponse(responseCode = "200", description = "Returned successfully."),
      @APIResponse(responseCode = "404", description = "Ref not found")
  })
  public EntriesResponse getEntries(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
      @Parameter(description = "name of ref to fetch from")
      @PathParam("ref")
          String refName,
      @Parameter(description = "maximum number of entries to return, just a hint for the server")
      @QueryParam("max")
          Integer maxRecords,
      @Parameter(description = "pagination continuation token, as returned in the previous EntriesResponse.token")
      @QueryParam("pageToken")
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
   * @see #getCommitLogStream(String, OptionalInt)
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("tree/{ref}/log")
  @Operation(summary = "Get commit log for a reference", description =
      "Retrieve the commit log for a ref, potentially truncated by the backend.\n"
          + "\n"
          + "Retrieves up to 'maxRecords' commit-log-entries starting at the HEAD of the "
          + "given named reference (tag or branch) or the given hash. "
          + "The backend may respect the given 'max' records hint, but return less or more entries. "
          + "Backends may also cap the returned entries at a hard-coded limit, the default "
          + "REST server implementation has such a hard-coded limit.\n"
          + "\n"
          + "To implement paging, check 'hasMore' in the response and, if 'true', pass the value "
          + "returned as 'token' in the next invocation as the 'pageToken' parameter.\n"
          + "\n"
          + "The content and meaning of the returned 'token' is \"private\" to the implementation,"
          + "treat is as an opaque value.\n"
          + "\n"
          + "It is wrong to assume that invoking this method with a very high 'maxRecords' value "
          + "will return all commit log entries.")
  @APIResponses({
      @APIResponse(responseCode = "200", description = "Returned commits."),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists")
  })
  LogResponse getCommitLog(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
      @Parameter(description = "ref to show log from")
      @PathParam("ref")
          String ref,
      @Parameter(description = "maximum number of commit-log entries to return, just a hint for the server")
      @QueryParam("max")
          Integer maxRecords,
      @Parameter(description = "pagination continuation token, as returned in the previous LogResponse.token")
      @QueryParam("pageToken")
          String pageToken)
          throws NessieNotFoundException;

  /**
   * Default implementation to return a stream of objects for a ref, functionally equivalent to
   * calling {@link #getEntries(String, Integer, String)} with manual paging.
   * <p>The {@link Stream} returned by {@code getEntriesStream(ref, OptionalInt.empty())},
   * if not limited, returns all commit-log entries.</p>
   *
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @param pageSizeHint page-size hint for the backend
   * @return stream of {@link Entry} objects
   */
  default Stream<Entry> getEntriesStream(@NotNull String ref, OptionalInt pageSizeHint) throws NessieNotFoundException {
    return new ResultStreamPaginator<>(EntriesResponse::getEntries, this::getEntries)
        .generateStream(ref, pageSizeHint);
  }

  /**
   * Default implementation to return a stream of commit-log entries, functionally equivalent to
   * calling {@link #getCommitLog(String, Integer, String)} with manual paging.
   * <p>The {@link Stream} returned by {@code getCommitLogStream(ref, OptionalInt.empty())},
   * if not limited, returns all commit-log entries.</p>
   *
   * @param ref a named reference (branch or tag name) or a commit-hash
   * @param pageSizeHint page-size hint for the backend
   * @return stream of {@link CommitMeta} objects
   */
  default Stream<CommitMeta> getCommitLogStream(@NotNull String ref, OptionalInt pageSizeHint) throws NessieNotFoundException {
    return new ResultStreamPaginator<>(LogResponse::getOperations, this::getCommitLog)
        .generateStream(ref, pageSizeHint);
  }

  /**
   * Update a tag.
   */
  @PUT
  @Path("tag/{tagName}")
  @Operation(summary = "Set a tag to a specific hash")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Assigned successfully"),
      @APIResponse(responseCode = "404", description = "One or more references don't exist"),
      @APIResponse(responseCode = "412", description = "Update conflict")
  })
  void assignTag(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Tag name to reassign")
      @PathParam("tagName")
          String tagName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected previous hash of tag")
      @QueryParam("expectedHash")
          String oldHash,
      @Valid
      @NotNull
      @RequestBody(description = "New tag content")
          Tag tag
      ) throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a tag.
   */
  @DELETE
  @Path("tag/{tagName}")
  @Operation(summary = "Delete a tag")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Deleted successfully."),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict"),
  })
  void deleteTag(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Tag to delete")
      @PathParam("tagName")
          String tagName,
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected hash of tag")
      @QueryParam("expectedHash")
          String hash
      ) throws NessieConflictException, NessieNotFoundException;

  /**
   * Update a branch.
   */
  @PUT
  @Path("branch/{branchName}")
  @Operation(summary = "Set a branch to a specific hash")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Assigned successfully"),
      @APIResponse(responseCode = "404", description = "One or more references don't exist"),
      @APIResponse(responseCode = "412", description = "Update conflict")
  })
  void assignBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Tag name to reassign")
      @PathParam("branchName")
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected previous hash of tag")
      @QueryParam("expectedHash")
          String oldHash,
      @Valid
      @NotNull
      @RequestBody(description = "New branch content")
          Branch branch
      ) throws NessieNotFoundException, NessieConflictException;

  /**
   * Delete a branch.
   */
  @DELETE
  @Path("branch/{branchName}")
  @Operation(summary = "Delete a branch endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Deleted successfully."),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict"),
  })
  void deleteBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Branch to delete")
      @PathParam("branchName")
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected hash of tag")
      @QueryParam("expectedHash")
          String hash
      ) throws NessieConflictException, NessieNotFoundException;

  /**
   * cherry pick a set of commits into a branch.
   */
  @POST
  @Path("branch/{branchName}/transplant")
  @Operation(summary = "transplant commits from mergeRef to ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Merged successfully."),
      @APIResponse(responseCode = "401", description = "no merge ref supplied"),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict")
  })
  void transplantCommitsIntoBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Branch to transplant into")
      @PathParam("branchName")
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected hash of tag")
      @QueryParam("expectedHash")
          String hash,
      @Parameter(description = "commit message")
      @QueryParam("message")
          String message,
      @Valid
      @RequestBody(description = "Hashes to transplant")
          Transplant transplant)
          throws NessieNotFoundException, NessieConflictException;

  /**
   * merge mergeRef onto ref.
   */
  @POST
  @Path("branch/{branchName}/merge")
  @Operation(summary = "merge commits from mergeRef to ref endpoint")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Merged successfully."),
      @APIResponse(responseCode = "401", description = "no merge ref supplied"),
      @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "update conflict")
  })
  void mergeRefIntoBranch(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Branch to merge into")
      @PathParam("branchName")
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected hash of tag")
      @QueryParam("expectedHash")
          String hash,
      @Valid
      @NotNull
      @RequestBody(description = "Merge operation")
          Merge merge)
          throws NessieNotFoundException, NessieConflictException;

  @POST
  @Path("branch/{branchName}/commit")
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(summary = "commit multiple on default branch")
  @APIResponses({
      @APIResponse(responseCode = "204", description = "Updated successfully."),
      @APIResponse(responseCode = "404", description = "Provided ref doesn't exists"),
      @APIResponse(responseCode = "412", description = "Update conflict")
  })
  public void commitMultipleOperations(
      @NotNull
      @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
      @Parameter(description = "Branch to change, defaults to default branch.")
      @PathParam("branchName")
          String branchName,
      @NotNull
      @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
      @Parameter(description = "Expected hash of branch.")
      @QueryParam("expectedHash")
          String hash,
      @Parameter(description = "Commit message")
      @QueryParam("message")
          String message,
      @Valid
      @NotNull
      @RequestBody(description = "Operations")
          Operations operations)
      throws NessieNotFoundException, NessieConflictException;
}
