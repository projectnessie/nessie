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
package org.projectnessie.api.v2.http;

import static org.projectnessie.api.v2.doc.ApiDoc.CHECKED_REF_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.CHECKED_REF_INFO;
import static org.projectnessie.api.v2.doc.ApiDoc.COMMIT_BRANCH_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.FOR_WRITE_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.KEY_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.MERGE_TRANSPLANT_BRANCH_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.PAGING_INFO;
import static org.projectnessie.api.v2.doc.ApiDoc.REF_NAME_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.REF_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.WITH_DOC_PARAMETER_DESCRIPTION;
import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_REGEX;

import com.fasterxml.jackson.annotation.JsonView;
import java.util.List;
import javax.validation.constraints.Pattern;
import javax.ws.rs.BeanParam;
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
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.projectnessie.api.v2.TreeApi;
import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.api.v2.params.ReferenceHistoryParams;
import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.api.v2.params.Transplant;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.Validation;
import org.projectnessie.model.ser.Views;

@Consumes(MediaType.APPLICATION_JSON)
@jakarta.ws.rs.Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
@Path("v2/trees")
@jakarta.ws.rs.Path("v2/trees")
@Tag(name = "v2")
public interface HttpTreeApi extends TreeApi {

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get information about all branches and tags",
      operationId = "getAllReferencesV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned references.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "referencesResponse"),
                  @ExampleObject(ref = "referencesResponseWithMetadata")
                },
                schema = @Schema(implementation = ReferencesResponse.class))),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
  })
  @JsonView(Views.V2.class)
  ReferencesResponse getAllReferences(@BeanParam @jakarta.ws.rs.BeanParam ReferencesParams params);

  @Override
  @POST
  @jakarta.ws.rs.POST
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Create a new branch or tag",
      description =
          "The name and type query parameters define the kind of reference to be created. "
              + "The payload object defines the new reference's origin in the commit history. "
              + "\n"
              + "Only branches and tags can be created by this method, but the payload object may be any"
              + " valid reference, including a detached commit."
              + "\n"
              + "If the payload reference object does not define a commit hash, the HEAD of that reference "
              + "will be used.",
      operationId = "createReferenceV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Created successfully.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {@ExampleObject(ref = "singleReferenceResponse")},
              schema = @Schema(implementation = SingleReferenceResponse.class))
        }),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to create reference"),
    @APIResponse(
        responseCode = "409",
        description = "Another reference with the same name already exists"),
  })
  @JsonView(Views.V2.class)
  SingleReferenceResponse createReference(
      @Parameter(required = true, description = REF_NAME_DESCRIPTION)
          @QueryParam("name")
          @jakarta.ws.rs.QueryParam("name")
          String name,
      @Parameter(
              required = true,
              description = "Type of the reference to be created",
              examples = {@ExampleObject(ref = "referenceType")})
          @QueryParam("type")
          @jakarta.ws.rs.QueryParam("type")
          String type,
      @RequestBody(
              required = true,
              description = "Source reference data from which the new reference is to be created.",
              content = {
                @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    examples = {@ExampleObject(ref = "refObjNew")})
              })
          Reference reference)
      throws NessieNotFoundException, NessieConflictException;

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}")
  @Operation(summary = "Fetch details of a reference", operationId = "getReferenceByNameV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Found and returned reference.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {
                @ExampleObject(ref = "singleReferenceResponse"),
                @ExampleObject(ref = "singleReferenceResponseWithMetadata")
              },
              schema = @Schema(implementation = SingleReferenceResponse.class))
        }),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view the given reference"),
    @APIResponse(responseCode = "404", description = "Ref not found")
  })
  @JsonView(Views.V2.class)
  SingleReferenceResponse getReferenceByName(
      @BeanParam @jakarta.ws.rs.BeanParam GetReferenceParams params) throws NessieNotFoundException;

  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/recent-changes")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/recent-changes")
  @Operation(
      summary = "Fetch recent pointer changes of a reference",
      operationId = "getReferenceHistory",
      description =
          "Retrieve the recorded recent history of a reference.\n"
              + "\n"
              + "A reference's history is a size and time limited record of changes of the reference's "
              + "current pointer, aka HEAD. The size and time limits are configured in the Nessie server "
              + "configuration.\n"
              + "\n"
              + "This function is only useful for deployments using replicating multi-zone/region database "
              + "setups. If the \"primary write target\" fails and cannot be recovered, replicas might not "
              + "have all written records (data loss scenario). This function helps identifying whether "
              + "the commits of a reference that were written within the configured \"replication lag\" are "
              + "present and consistent. A reference might then be deleted or re-assigned to a consistent commit.")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Found and returned reference.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {@ExampleObject(ref = "referenceHistoryResponse")},
              schema = @Schema(implementation = ReferenceHistoryResponse.class))
        }),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view the given reference"),
    @APIResponse(responseCode = "404", description = "Reference not found")
  })
  @JsonView(Views.V2.class)
  @Override
  ReferenceHistoryResponse getReferenceHistory(
      @BeanParam @jakarta.ws.rs.BeanParam ReferenceHistoryParams params)
      throws NessieNotFoundException;

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/entries")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/entries")
  @Operation(
      summary = "Fetch all entries for a given reference",
      description =
          "Retrieves objects for a ref, potentially truncated by the backend.\n"
              + "\n"
              + "Retrieves up to 'maxRecords' entries for the "
              + "given named reference (tag or branch) or the given hash. "
              + "The backend may respect the given 'max' records hint, but return less or more entries. "
              + "Backends may also cap the returned entries at a hard-coded limit, the default "
              + "REST server implementation has such a hard-coded limit.\n"
              + "\n"
              + PAGING_INFO
              + "\n"
              + "The 'filter' parameter allows for advanced filtering capabilities using the Common Expression Language (CEL).\n"
              + "An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n",
      operationId = "getEntriesV2")
  @APIResponses({
    @APIResponse(
        description = "List names and object types in a contents tree",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {@ExampleObject(ref = "entriesResponseV2")},
              schema = @Schema(implementation = EntriesResponse.class))
        }),
    @APIResponse(responseCode = "200", description = "Returned successfully."),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or fetch entries for it"),
    @APIResponse(responseCode = "404", description = "Ref not found")
  })
  @JsonView(Views.V2.class)
  EntriesResponse getEntries(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = REF_PARAMETER_DESCRIPTION,
              examples = {
                @ExampleObject(ref = "ref"),
                @ExampleObject(ref = "refWithHash"),
                @ExampleObject(
                    ref = "refWithTimestampMillisSinceEpoch",
                    description =
                        "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
                @ExampleObject(
                    ref = "refWithTimestampInstant",
                    description = "The commit 'valid for' the given ISO-8601 instant on main"),
                @ExampleObject(
                    ref = "refWithNthPredecessor",
                    description = "The 10th commit from HEAD of main"),
                @ExampleObject(
                    ref = "refWithMergeParent",
                    description =
                        "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
                @ExampleObject(ref = "refDefault"),
                @ExampleObject(ref = "refDetached"),
              })
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref,
      @BeanParam @jakarta.ws.rs.BeanParam EntriesParams params)
      throws NessieNotFoundException;

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history")
  @Operation(
      summary = "Get commit log for a particular reference",
      description =
          "Retrieve the commit log for a reference, potentially truncated by the backend.\n"
              + "\n"
              + "The backend may respect the given 'max-entries' records hint, or may return more or less entries. "
              + "Backends may also cap the returned entries at a hard-coded limit\n"
              + "\n"
              + PAGING_INFO
              + "\n"
              + "The 'filter' parameter allows for advanced filtering capabilities using the Common Expression Language (CEL).\n"
              + "An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
              + "\n"
              + "The fetching of the log starts from the HEAD of the given ref (or a more specific commit, if provided "
              + "as part of the 'ref' path element) and proceeds until the 'root' commit or the 'limit-hash' commit "
              + "are encountered.",
      operationId = "getCommitLogV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned commits.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {
                @ExampleObject(ref = "logResponseAdditionalInfo"),
                @ExampleObject(ref = "logResponseSimple")
              },
              schema = @Schema(implementation = LogResponse.class))
        }),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or get commit log for it"),
    @APIResponse(responseCode = "404", description = "Ref doesn't exists")
  })
  @JsonView(Views.V2.class)
  LogResponse getCommitLog(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = REF_PARAMETER_DESCRIPTION,
              examples = {
                @ExampleObject(ref = "ref"),
                @ExampleObject(ref = "refWithHash"),
                @ExampleObject(
                    ref = "refWithTimestampMillisSinceEpoch",
                    description =
                        "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
                @ExampleObject(
                    ref = "refWithTimestampInstant",
                    description = "The commit 'valid for' the given ISO-8601 instant on main"),
                @ExampleObject(
                    ref = "refWithNthPredecessor",
                    description = "The 10th commit from HEAD of main"),
                @ExampleObject(
                    ref = "refWithMergeParent",
                    description =
                        "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
                @ExampleObject(ref = "refDefault"),
                @ExampleObject(ref = "refDetached"),
              })
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref,
      @BeanParam @jakarta.ws.rs.BeanParam CommitLogParams params)
      throws NessieNotFoundException;

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path(
      "{from-ref:"
          + REF_NAME_PATH_ELEMENT_REGEX
          + "}/diff/{to-ref:"
          + REF_NAME_PATH_ELEMENT_REGEX
          + "}")
  @jakarta.ws.rs.Path(
      "{from-ref:"
          + REF_NAME_PATH_ELEMENT_REGEX
          + "}/diff/{to-ref:"
          + REF_NAME_PATH_ELEMENT_REGEX
          + "}")
  @Operation(
      summary = "Get contents that differ in the trees specified by the two given references",
      description =
          "The URL pattern is basically 'from' and 'to' reference specs separated by '/diff/'\n"
              + "\n"
              + "Examples: \n"
              + "- main/diff/myBranch\n"
              + "- main@1234567890123456/diff/myBranch\n"
              + "- main@1234567890123456/diff/myBranch@23445678\n"
              + "- main/diff/myBranch@23445678\n"
              + "- main/diff/myBranch@23445678\n"
              + "- my/branch@/diff/main\n"
              + "- myBranch/diff/-\n",
      operationId = "getDiffV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Returned diff for the given references.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {
                  @ExampleObject(ref = "diffResponseWithRef"),
                },
                schema = @Schema(implementation = DiffResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, fromRef/toRef name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view the given fromRef/toRef"),
    @APIResponse(responseCode = "404", description = "fromRef/toRef not found"),
  })
  @JsonView(Views.V2.class)
  DiffResponse getDiff(@BeanParam @jakarta.ws.rs.BeanParam DiffParams params)
      throws NessieNotFoundException;

  @Override
  @PUT
  @jakarta.ws.rs.PUT
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}")
  @Operation(
      summary = "Set a named reference to a specific hash via another reference.",
      description =
          "The 'ref' parameter identifies the branch or tag to be reassigned.\n"
              + CHECKED_REF_INFO
              + "\n"
              + "Only branches and tags may be reassigned."
              + "\n"
              + "The payload object identifies any reference visible to the current user whose 'hash' will be used to "
              + "define the new HEAD of the reference being reassigned. Detached hashes may be used in the payload.",
      operationId = "assignReferenceV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Assigned successfully.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {@ExampleObject(ref = "singleReferenceResponse")},
              schema = @Schema(implementation = SingleReferenceResponse.class))
        }),
    @APIResponse(responseCode = "400", description = "Invalid input, ref specification not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view or assign reference"),
    @APIResponse(responseCode = "404", description = "One or more references don't exist"),
    @APIResponse(
        responseCode = "409",
        description = "Update conflict or expected hash / type mismatch")
  })
  @JsonView(Views.V2.class)
  SingleReferenceResponse assignReference(
      @Parameter(
              description = "Optional expected type of the reference being reassigned",
              examples = {@ExampleObject(ref = "referenceType")})
          @QueryParam("type")
          @jakarta.ws.rs.QueryParam("type")
          String type,
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = CHECKED_REF_DESCRIPTION,
              examples = @ExampleObject(ref = "refWithHash"))
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref,
      @RequestBody(
              required = true,
              description =
                  "Reference to which the 'ref' (from the path parameter) shall be assigned. This must be either a "
                      + "'Detached' commit, 'Branch' or 'Tag' via which the hash is visible to the caller.",
              content =
                  @Content(
                      mediaType = MediaType.APPLICATION_JSON,
                      examples = {@ExampleObject(ref = "refObj"), @ExampleObject(ref = "tagObj")}))
          Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  @Override
  @DELETE
  @jakarta.ws.rs.DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}")
  @Operation(
      summary = "Delete a reference",
      description =
          "The 'ref' parameter identifies the branch or tag to be deleted.\n"
              + CHECKED_REF_INFO
              + "\n"
              + "Only branches and tags can be deleted. However, deleting the default branch may be restricted.",
      operationId = "deleteReferenceV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Deleted successfully.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {@ExampleObject(ref = "singleReferenceResponse")},
              schema = @Schema(implementation = SingleReferenceResponse.class))
        }),
    @APIResponse(responseCode = "400", description = "Invalid input, ref/hash name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(responseCode = "403", description = "Not allowed to view or delete reference"),
    @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
    @APIResponse(responseCode = "409", description = "update conflict"),
  })
  @JsonView(Views.V2.class)
  SingleReferenceResponse deleteReference(
      @Parameter(
              description = "Optional expected type of the reference being deleted",
              examples = {@ExampleObject(ref = "referenceType")})
          @QueryParam("type")
          @jakarta.ws.rs.QueryParam("type")
          String type,
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = CHECKED_REF_DESCRIPTION,
              examples = @ExampleObject(ref = "refWithHash"))
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref)
      throws NessieConflictException, NessieNotFoundException;

  @Override
  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/contents/{key}")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/contents/{key}")
  @Operation(
      summary = "Get the content object associated with a key.",
      description =
          "This operation returns the content value for a content key at a particular point in history as defined "
              + "by the 'ref' parameter.",
      operationId = "getContentV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Information for a table, view or another content object for the given key",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = {@ExampleObject(ref = "contentResponseIceberg")},
                schema = @Schema(implementation = ContentResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or read object content for a key"),
    @APIResponse(
        responseCode = "404",
        description = "Table not found on 'ref' or non-existent reference")
  })
  @JsonView(Views.V2.class)
  ContentResponse getContent(
      @Parameter(description = KEY_PARAMETER_DESCRIPTION)
          @PathParam("key")
          @jakarta.ws.rs.PathParam("key")
          ContentKey key,
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = REF_PARAMETER_DESCRIPTION,
              examples = {
                @ExampleObject(ref = "ref"),
                @ExampleObject(ref = "refWithHash"),
                @ExampleObject(
                    ref = "refWithTimestampMillisSinceEpoch",
                    description =
                        "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
                @ExampleObject(
                    ref = "refWithTimestampInstant",
                    description = "The commit 'valid for' the given ISO-8601 instant on main"),
                @ExampleObject(
                    ref = "refWithNthPredecessor",
                    description = "The 10th commit from HEAD of main"),
                @ExampleObject(
                    ref = "refWithMergeParent",
                    description =
                        "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
                @ExampleObject(ref = "refDefault"),
                @ExampleObject(ref = "refDetached"),
              })
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref,
      @Parameter(description = WITH_DOC_PARAMETER_DESCRIPTION)
          @QueryParam("with-doc")
          @jakarta.ws.rs.QueryParam("with-doc")
          boolean withDocumentation,
      @Parameter(description = FOR_WRITE_PARAMETER_DESCRIPTION)
          @QueryParam("for-write")
          @jakarta.ws.rs.QueryParam("for-write")
          boolean forWrite)
      throws NessieNotFoundException;

  @GET
  @jakarta.ws.rs.GET
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/contents")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/contents")
  @Operation(
      summary = "Get multiple content objects.",
      description =
          "Similar to 'GET /trees/{ref}/content/{key}', but takes multiple 'key' query parameters and returns zero "
              + "or more content values in the same JSON structure as the 'POST /trees/{ref}/content' endpoint.\n"
              + "\n"
              + "This is a convenience method for fetching a small number of content objects. It is mostly intended "
              + "for human use. For automated use cases or when the number of keys is large the "
              + "'POST /trees/{ref}/content' method is preferred.")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Retrieved successfully.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(ref = "multipleContentsResponse"),
                schema = @Schema(implementation = GetMultipleContentsResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or read object content for a key"),
    @APIResponse(responseCode = "404", description = "Provided ref doesn't exists")
  })
  @JsonView(Views.V2.class)
  GetMultipleContentsResponse getSeveralContents(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = REF_PARAMETER_DESCRIPTION,
              examples = {
                @ExampleObject(ref = "ref"),
                @ExampleObject(ref = "refWithHash"),
                @ExampleObject(
                    ref = "refWithTimestampMillisSinceEpoch",
                    description =
                        "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
                @ExampleObject(
                    ref = "refWithTimestampInstant",
                    description = "The commit 'valid for' the given ISO-8601 instant on main"),
                @ExampleObject(
                    ref = "refWithNthPredecessor",
                    description = "The 10th commit from HEAD of main"),
                @ExampleObject(
                    ref = "refWithMergeParent",
                    description =
                        "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
                @ExampleObject(ref = "refDefault"),
                @ExampleObject(ref = "refDetached"),
              })
          @Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          @jakarta.validation.constraints.Pattern(
              regexp = Validation.REF_NAME_PATH_REGEX,
              message = Validation.REF_NAME_PATH_MESSAGE)
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref,
      @Parameter(description = KEY_PARAMETER_DESCRIPTION)
          @QueryParam("key")
          @jakarta.ws.rs.QueryParam("key")
          List<String> keys,
      @Parameter(description = WITH_DOC_PARAMETER_DESCRIPTION)
          @QueryParam("with-doc")
          @jakarta.ws.rs.QueryParam("with-doc")
          boolean withDocumentation,
      @Parameter(description = FOR_WRITE_PARAMETER_DESCRIPTION)
          @QueryParam("for-write")
          @jakarta.ws.rs.QueryParam("for-write")
          boolean forWrite)
      throws NessieNotFoundException;

  @Override
  @POST
  @jakarta.ws.rs.POST
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/contents")
  @jakarta.ws.rs.Path("{ref:" + REF_NAME_PATH_ELEMENT_REGEX + "}/contents")
  @Operation(
      summary = "Get multiple content objects.",
      description =
          "Similar to 'GET /trees/{ref}/content/{key}', but takes multiple 'ContentKey's (in the JSON payload) and "
              + "returns zero or more content objects.\n"
              + "\n"
              + "Note that if some keys from the request do not have an associated content object at the point in "
              + "history defined by the 'ref' parameter, the response will be successful, but no data will be "
              + "returned for the missing keys.",
      operationId = "getMultipleContentsV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Retrieved successfully.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(ref = "multipleContentsResponse"),
                schema = @Schema(implementation = GetMultipleContentsResponse.class))),
    @APIResponse(responseCode = "400", description = "Invalid input, ref name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or read object content for a key"),
    @APIResponse(responseCode = "404", description = "Provided ref doesn't exists")
  })
  @JsonView(Views.V2.class)
  GetMultipleContentsResponse getMultipleContents(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = REF_PARAMETER_DESCRIPTION,
              examples = {
                @ExampleObject(ref = "ref"),
                @ExampleObject(ref = "refWithHash"),
                @ExampleObject(
                    ref = "refWithTimestampMillisSinceEpoch",
                    description =
                        "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
                @ExampleObject(
                    ref = "refWithTimestampInstant",
                    description = "The commit 'valid for' the given ISO-8601 instant on main"),
                @ExampleObject(
                    ref = "refWithNthPredecessor",
                    description = "The 10th commit from HEAD of main"),
                @ExampleObject(
                    ref = "refWithMergeParent",
                    description =
                        "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
                @ExampleObject(ref = "refDefault"),
                @ExampleObject(ref = "refDetached"),
              })
          @PathParam("ref")
          @jakarta.ws.rs.PathParam("ref")
          String ref,
      @RequestBody(
              description = "Keys to retrieve.",
              content = @Content(examples = @ExampleObject(ref = "multiGetRequest")))
          GetMultipleContentsRequest request,
      @Parameter(description = WITH_DOC_PARAMETER_DESCRIPTION)
          @QueryParam("with-doc")
          @jakarta.ws.rs.QueryParam("with-doc")
          boolean withDocumentation,
      @Parameter(description = FOR_WRITE_PARAMETER_DESCRIPTION)
          @QueryParam("for-write")
          @jakarta.ws.rs.QueryParam("for-write")
          boolean forWrite)
      throws NessieNotFoundException;

  @Override
  @POST
  @jakarta.ws.rs.POST
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{branch:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history/transplant")
  @jakarta.ws.rs.Path("{branch:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history/transplant")
  @Operation(
      summary =
          "Transplant commits specified by the 'Transplant' payload object onto the given 'branch'",
      description =
          "This is done as an atomic operation such that only the last of the sequence is ever "
              + "visible to concurrent readers/writers. The sequence to transplant must be "
              + "contiguous and in order.\n"
              + "\n"
              + "The state of contents specified by the 'branch' reference will be used for detecting conflicts with "
              + "the commits being transplanted.",
      operationId = "transplantV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(ref = "mergeResponseSuccess"),
                schema = @Schema(implementation = MergeResponse.class)),
        description =
            "Transplant operation completed. "
                + "The actual transplant might have failed and reported as successful=false, "
                + "if the client asked to return a conflict as a result instead of returning an error. "
                + "Note: the 'commonAncestor' field in a response will always be null for a transplant."),
    @APIResponse(responseCode = "400", description = "Invalid input, ref/hash name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or transplant commits"),
    @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
    @APIResponse(
        responseCode = "409",
        description = "Transplant conflict",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(ref = "mergeResponseFail"),
                schema = @Schema(implementation = MergeResponse.class)))
  })
  @JsonView(Views.V2.class)
  MergeResponse transplantCommitsIntoBranch(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = MERGE_TRANSPLANT_BRANCH_DESCRIPTION,
              examples = @ExampleObject(ref = "refWithHash"))
          @PathParam("branch")
          @jakarta.ws.rs.PathParam("branch")
          String branch,
      @RequestBody(
              required = true,
              description = "Commits to transplant",
              content =
                  @Content(
                      mediaType = MediaType.APPLICATION_JSON,
                      examples = {@ExampleObject(ref = "transplant")}))
          Transplant transplant)
      throws NessieNotFoundException, NessieConflictException;

  @Override
  @POST
  @jakarta.ws.rs.POST
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Path("{branch:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history/merge")
  @jakarta.ws.rs.Path("{branch:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history/merge")
  @Operation(
      summary = "Merge commits from another reference onto 'branch'.",
      description =
          "Merge commits referenced by the 'mergeRefName' and 'fromHash' parameters of the payload object into the "
              + "requested 'branch'.\n"
              + "\n"
              + "The state of contents specified by the 'branch' reference will be used for detecting conflicts with "
              + "the commits being transplanted.\n"
              + "\n"
              + "The merge is committed if it is free from conflicts. The set of commits merged into the target branch "
              + "will be all of those starting at 'fromHash' on 'mergeRefName' until we arrive at the common ancestor. "
              + "Depending on the underlying implementation, the number of commits allowed as part of this operation "
              + "may be limited.",
      operationId = "mergeV2")
  @APIResponses({
    @APIResponse(
        responseCode = "204",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(ref = "mergeResponseSuccess"),
                schema = @Schema(implementation = MergeResponse.class)),
        description =
            "Merge operation completed. "
                + "The actual merge might have failed and reported as successful=false, "
                + "if the client asked to return a conflict as a result instead of returning an error."),
    @APIResponse(responseCode = "400", description = "Invalid input, ref/hash name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or merge commits"),
    @APIResponse(responseCode = "404", description = "Ref doesn't exists"),
    @APIResponse(
        responseCode = "409",
        description = "Merge conflict",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                examples = @ExampleObject(ref = "mergeResponseFail"),
                schema = @Schema(implementation = MergeResponse.class)))
  })
  @JsonView(Views.V2.class)
  MergeResponse mergeRefIntoBranch(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = MERGE_TRANSPLANT_BRANCH_DESCRIPTION,
              examples = @ExampleObject(ref = "refWithHash"))
          @PathParam("branch")
          @jakarta.ws.rs.PathParam("branch")
          String branch,
      @RequestBody(
              required = true,
              description =
                  "Merge operation that defines the source reference name and an optional hash. "
                      + "If 'fromHash' is not present, the current 'sourceRef's HEAD will be used.",
              content =
                  @Content(
                      mediaType = MediaType.APPLICATION_JSON,
                      examples = {@ExampleObject(ref = "merge")}))
          Merge merge)
      throws NessieNotFoundException, NessieConflictException;

  @Override
  @POST
  @jakarta.ws.rs.POST
  @Path("{branch:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history/commit")
  @jakarta.ws.rs.Path("{branch:" + REF_NAME_PATH_ELEMENT_REGEX + "}/history/commit")
  @Produces(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Produces(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @jakarta.ws.rs.Consumes(jakarta.ws.rs.core.MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Commit one or more operations against the given 'branch'.",
      description =
          "The state of contents specified by the 'branch' reference will be used for detecting conflicts with "
              + "the operation being committed.\n"
              + "\n"
              + "The hash in the successful response will be the hash of the commit that contains the requested "
              + "operations, whose immediate parent commit will be the current HEAD of the specified branch.",
      operationId = "commitV2")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "Updated successfully.",
        content = {
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              examples = {@ExampleObject(ref = "commitResponse")},
              schema = @Schema(implementation = CommitResponse.class))
        }),
    @APIResponse(responseCode = "400", description = "Invalid input, ref/hash name not valid"),
    @APIResponse(responseCode = "401", description = "Invalid credentials provided"),
    @APIResponse(
        responseCode = "403",
        description = "Not allowed to view the given reference or perform commits"),
    @APIResponse(responseCode = "404", description = "Provided ref doesn't exist"),
    @APIResponse(responseCode = "409", description = "Update conflict")
  })
  @JsonView(Views.V2.class)
  CommitResponse commitMultipleOperations(
      @Parameter(
              schema = @Schema(pattern = REF_NAME_PATH_ELEMENT_REGEX),
              description = COMMIT_BRANCH_DESCRIPTION,
              examples = @ExampleObject(ref = "refWithHash"))
          @PathParam("branch")
          @jakarta.ws.rs.PathParam("branch")
          String branch,
      @RequestBody(
              required = true,
              description = "Operations to commit",
              content =
                  @Content(
                      mediaType = MediaType.APPLICATION_JSON,
                      examples = {@ExampleObject(ref = "operations")}))
          Operations operations)
      throws NessieNotFoundException, NessieConflictException;
}
