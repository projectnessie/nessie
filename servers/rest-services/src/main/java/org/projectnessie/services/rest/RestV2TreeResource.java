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
package org.projectnessie.services.rest;

import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.services.spi.TreeService.MAX_COMMIT_LOG_ENTRIES;

import com.fasterxml.jackson.annotation.JsonView;
import java.util.List;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.v2.http.HttpTreeApi;
import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.api.v2.params.Transplant;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.Tag;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.spi.ConfigService;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedCountingResponseHandler;
import org.projectnessie.services.spi.TreeService;

/** REST endpoint for the tree-API. */
@RequestScoped
public class RestV2TreeResource implements HttpTreeApi {

  private final ConfigService configService;
  private final TreeService treeService;
  private final ContentService contentService;
  private final DiffService diffService;

  // Mandated by CDI 2.0
  public RestV2TreeResource() {
    this(null, null, null, null);
  }

  @Inject
  @jakarta.inject.Inject
  public RestV2TreeResource(
      ConfigService configService,
      TreeService treeService,
      ContentService contentService,
      DiffService diffService) {
    this.configService = configService;
    this.treeService = treeService;
    this.contentService = contentService;
    this.diffService = diffService;
  }

  private Reference resolveRef(String refPathString) {
    return resolveReferencePathElement(refPathString, configService::getConfig);
  }

  private TreeService tree() {
    return treeService;
  }

  private DiffService diff() {
    return diffService;
  }

  private ContentService content() {
    return contentService;
  }

  @JsonView(Views.V2.class)
  @Override
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    Integer maxRecords = params.maxRecords();
    return tree()
        .getAllReferences(
            params.fetchOption(),
            params.filter(),
            params.pageToken(),
            new PagedCountingResponseHandler<ReferencesResponse, Reference>(maxRecords) {
              final ImmutableReferencesResponse.Builder builder = ReferencesResponse.builder();

              @Override
              public ReferencesResponse build() {
                return builder.build();
              }

              @Override
              protected boolean doAddEntry(Reference entry) {
                builder.addReferences(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            });
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse createReference(
      String name, Reference.ReferenceType type, Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    String fromRefName = null;
    String fromHash = null;
    if (reference != null) {
      fromRefName = reference.getName();
      fromHash = reference.getHash();
    }

    Reference created = tree().createReference(name, type, fromHash, fromRefName);
    return SingleReferenceResponse.builder().reference(created).build();
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse getReferenceByName(GetReferenceParams params)
      throws NessieNotFoundException {
    Reference reference = resolveRef(params.getRef());
    return SingleReferenceResponse.builder()
        .reference(tree().getReferenceByName(reference.getName(), params.fetchOption()))
        .build();
  }

  @JsonView(Views.V2.class)
  @Override
  public EntriesResponse getEntries(String ref, EntriesParams params)
      throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    Integer maxRecords = params.maxRecords();
    ImmutableEntriesResponse.Builder builder = EntriesResponse.builder();
    return tree()
        .getEntries(
            reference.getName(),
            reference.getHash(),
            null,
            params.filter(),
            params.pageToken(),
            params.withContent(),
            new PagedCountingResponseHandler<EntriesResponse, EntriesResponse.Entry>(maxRecords) {
              @Override
              public EntriesResponse build() {
                return builder.build();
              }

              @Override
              protected boolean doAddEntry(EntriesResponse.Entry entry) {
                builder.addEntries(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            },
            h -> builder.effectiveReference(toReference(h)));
  }

  @JsonView(Views.V2.class)
  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    Integer maxRecords = params.maxRecords();
    return tree()
        .getCommitLog(
            reference.getName(),
            params.fetchOption(),
            params.startHash(),
            reference.getHash(),
            params.filter(),
            params.pageToken(),
            new PagedCountingResponseHandler<LogResponse, LogEntry>(
                maxRecords, MAX_COMMIT_LOG_ENTRIES) {
              final ImmutableLogResponse.Builder builder = ImmutableLogResponse.builder();

              @Override
              public LogResponse build() {
                return builder.build();
              }

              @Override
              protected boolean doAddEntry(LogEntry entry) {
                builder.addLogEntries(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            });
  }

  @JsonView(Views.V2.class)
  @Override
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    Integer maxRecords = params.maxRecords();
    Reference from = resolveRef(params.getFromRef());
    Reference to = resolveRef(params.getToRef());
    ImmutableDiffResponse.Builder builder = DiffResponse.builder();
    return diff()
        .getDiff(
            from.getName(),
            from.getHash(),
            to.getName(),
            to.getHash(),
            params.pageToken(),
            new PagedCountingResponseHandler<DiffResponse, DiffEntry>(maxRecords) {
              @Override
              public DiffResponse build() {
                return builder.build();
              }

              @Override
              protected boolean doAddEntry(DiffEntry entry) {
                builder.addDiffs(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            },
            h -> builder.effectiveFromReference(toReference(h)),
            h -> builder.effectiveToReference(toReference(h)));
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse assignReference(
      Reference.ReferenceType type, String ref, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    Reference reference = resolveReferencePathElement(ref, type);
    Reference updated =
        tree().assignReference(type, reference.getName(), reference.getHash(), assignTo);
    return SingleReferenceResponse.builder().reference(updated).build();
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse deleteReference(Reference.ReferenceType type, String ref)
      throws NessieConflictException, NessieNotFoundException {
    Reference reference = resolveReferencePathElement(ref, type);
    String hash = tree().deleteReference(type, reference.getName(), reference.getHash());
    if (reference instanceof Branch) {
      reference = Branch.of(reference.getName(), hash);
    }
    if (reference instanceof Tag) {
      reference = Tag.of(reference.getName(), hash);
    }
    return SingleReferenceResponse.builder().reference(reference).build();
  }

  @JsonView(Views.V2.class)
  @Override
  public ContentResponse getContent(ContentKey key, String ref) throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    return content().getContent(key, reference.getName(), reference.getHash());
  }

  @JsonView(Views.V2.class)
  @Override
  public GetMultipleContentsResponse getSeveralContents(String ref, List<String> keys)
      throws NessieNotFoundException {
    ImmutableGetMultipleContentsRequest.Builder request = GetMultipleContentsRequest.builder();
    keys.forEach(k -> request.addRequestedKeys(ContentKey.fromPathString(k)));
    return getMultipleContents(ref, request.build());
  }

  @JsonView(Views.V2.class)
  @Override
  public GetMultipleContentsResponse getMultipleContents(
      String ref, GetMultipleContentsRequest request) throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    return content()
        .getMultipleContents(reference.getName(), reference.getHash(), request.getRequestedKeys());
  }

  @JsonView(Views.V2.class)
  @Override
  public MergeResponse transplantCommitsIntoBranch(String branch, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    Reference ref = resolveRef(branch);
    return tree()
        .transplantCommitsIntoBranch(
            ref.getName(),
            ref.getHash(),
            transplant.getMessage(),
            transplant.getHashesToTransplant(),
            transplant.getFromRefName(),
            true,
            transplant.getKeyMergeModes(),
            transplant.getDefaultKeyMergeMode(),
            transplant.isDryRun(),
            transplant.isFetchAdditionalInfo(),
            transplant.isReturnConflictAsResult());
  }

  @JsonView(Views.V2.class)
  @Override
  public MergeResponse mergeRefIntoBranch(String branch, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    Reference ref = resolveRef(branch);
    return tree()
        .mergeRefIntoBranch(
            ref.getName(),
            ref.getHash(),
            merge.getFromRefName(),
            merge.getFromHash(),
            false,
            merge.getMessage(),
            merge.getKeyMergeModes(),
            merge.getDefaultKeyMergeMode(),
            merge.isDryRun(),
            merge.isFetchAdditionalInfo(),
            merge.isReturnConflictAsResult());
  }

  @JsonView(Views.V2.class)
  @Override
  public CommitResponse commitMultipleOperations(String branch, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    Reference ref = resolveRef(branch);
    return tree().commitMultipleOperations(ref.getName(), ref.getHash(), operations);
  }
}
