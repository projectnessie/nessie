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
package org.projectnessie.services.rest;

import static org.projectnessie.services.spi.TreeService.MAX_COMMIT_LOG_ENTRIES;

import com.fasterxml.jackson.annotation.JsonView;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.v1.http.HttpTreeApi;
import org.projectnessie.api.v1.params.CommitLogParams;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.api.v1.params.GetReferenceParams;
import org.projectnessie.api.v1.params.Merge;
import org.projectnessie.api.v1.params.ReferencesParams;
import org.projectnessie.api.v1.params.Transplant;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.services.spi.TreeService;

/** REST endpoint for the tree-API. */
@RequestScoped
public class RestTreeResource implements HttpTreeApi {
  // Cannot extend the TreeApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpTreeApi or the plain TreeApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final TreeService treeService;

  // Mandated by CDI 2.0
  public RestTreeResource() {
    this(null);
  }

  @Inject
  public RestTreeResource(TreeService treeService) {
    this.treeService = treeService;
  }

  private TreeService resource() {
    return treeService;
  }

  @JsonView(Views.V1.class)
  @Override
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    Integer maxRecords = params.maxRecords();
    return resource()
        .getAllReferences(
            params.fetchOption(),
            params.filter(),
            null,
            new PagedResponseHandler<
                ImmutableReferencesResponse.Builder, ReferencesResponse, Reference>() {
              final int max = maxRecords != null ? Math.max(maxRecords, 0) : 0;

              @Override
              public ImmutableReferencesResponse.Builder newBuilder() {
                return ReferencesResponse.builder();
              }

              @Override
              public ReferencesResponse build(ImmutableReferencesResponse.Builder builder) {
                return builder.build();
              }

              @Override
              public boolean addEntry(
                  ImmutableReferencesResponse.Builder builder, int cnt, Reference entry) {
                if (max > 0 && cnt > max) {
                  return false;
                }
                builder.addReferences(entry);
                return true;
              }

              @Override
              public void hasMore(ImmutableReferencesResponse.Builder builder, String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            });
  }

  @JsonView(Views.V1.class)
  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    return resource().getDefaultBranch();
  }

  @JsonView(Views.V1.class)
  @Override
  public Reference createReference(String sourceRefName, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    return resource()
        .createReference(
            reference.getName(), reference.getType(), reference.getHash(), sourceRefName);
  }

  @JsonView(Views.V1.class)
  @Override
  public Reference getReferenceByName(GetReferenceParams params) throws NessieNotFoundException {
    return resource().getReferenceByName(params.getRefName(), params.fetchOption());
  }

  @JsonView(Views.V1.class)
  @Override
  public EntriesResponse getEntries(String refName, EntriesParams params)
      throws NessieNotFoundException {
    Integer maxRecords = params.maxRecords();
    return resource()
        .getEntries(
            refName,
            params.hashOnRef(),
            params.namespaceDepth(),
            params.filter(),
            params.pageToken(),
            new PagedResponseHandler<
                ImmutableEntriesResponse.Builder, EntriesResponse, EntriesResponse.Entry>() {
              final int max = maxRecords != null ? maxRecords : Integer.MAX_VALUE;

              @Override
              public ImmutableEntriesResponse.Builder newBuilder() {
                return ImmutableEntriesResponse.builder();
              }

              @Override
              public EntriesResponse build(ImmutableEntriesResponse.Builder builder) {
                return builder.build();
              }

              @Override
              public boolean addEntry(
                  ImmutableEntriesResponse.Builder builder, int cnt, EntriesResponse.Entry entry) {
                if (max > 0 && cnt > max) {
                  return false;
                }
                builder.addEntries(entry);
                return true;
              }

              @Override
              public void hasMore(ImmutableEntriesResponse.Builder builder, String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            });
  }

  @JsonView(Views.V1.class)
  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    Integer maxRecords = params.maxRecords();
    return resource()
        .getCommitLog(
            ref,
            params.fetchOption(),
            params.startHash(),
            params.endHash(),
            params.filter(),
            params.pageToken(),
            new PagedResponseHandler<ImmutableLogResponse.Builder, LogResponse, LogEntry>() {
              final int max =
                  Math.min(
                      maxRecords != null ? Math.max(maxRecords, 0) : MAX_COMMIT_LOG_ENTRIES,
                      MAX_COMMIT_LOG_ENTRIES);

              @Override
              public ImmutableLogResponse.Builder newBuilder() {
                return ImmutableLogResponse.builder();
              }

              @Override
              public LogResponse build(ImmutableLogResponse.Builder builder) {
                return builder.build();
              }

              @Override
              public boolean addEntry(
                  ImmutableLogResponse.Builder builder, int cnt, LogEntry entry) {
                if (max > 0 && cnt > max) {
                  return false;
                }
                builder.addLogEntries(entry);
                return true;
              }

              @Override
              public void hasMore(ImmutableLogResponse.Builder builder, String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            });
  }

  @JsonView(Views.V1.class)
  @Override
  public void assignReference(
      Reference.ReferenceType referenceType,
      String referenceName,
      String expectedHash,
      Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    resource().assignReference(referenceType, referenceName, expectedHash, assignTo);
  }

  @JsonView(Views.V1.class)
  @Override
  public void deleteReference(
      Reference.ReferenceType referenceType, String referenceName, String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    resource().deleteReference(referenceType, referenceName, expectedHash);
  }

  @JsonView(Views.V1.class)
  @Override
  public MergeResponse transplantCommitsIntoBranch(
      String branchName, String expectedHash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    return resource()
        .transplantCommitsIntoBranch(
            branchName,
            expectedHash,
            message,
            transplant.getHashesToTransplant(),
            transplant.getFromRefName(),
            transplant.keepIndividualCommits(),
            transplant.getKeyMergeModes(),
            transplant.getDefaultKeyMergeMode(),
            transplant.isDryRun(),
            transplant.isFetchAdditionalInfo(),
            transplant.isReturnConflictAsResult());
  }

  @JsonView(Views.V1.class)
  @Override
  public MergeResponse mergeRefIntoBranch(String branchName, String expectedHash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    return resource()
        .mergeRefIntoBranch(
            branchName,
            expectedHash,
            merge.getFromRefName(),
            merge.getFromHash(),
            merge.keepIndividualCommits(),
            null,
            merge.getKeyMergeModes(),
            merge.getDefaultKeyMergeMode(),
            merge.isDryRun(),
            merge.isFetchAdditionalInfo(),
            merge.isReturnConflictAsResult());
  }

  @JsonView(Views.V1.class)
  @Override
  public Branch commitMultipleOperations(
      String branchName, String expectedHash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    return resource()
        .commitMultipleOperations(branchName, expectedHash, operations)
        .getTargetBranch();
  }
}
