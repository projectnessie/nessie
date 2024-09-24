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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;
import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.services.rest.RestApiContext.NESSIE_V2;
import static org.projectnessie.services.rest.common.RestCommon.updateCommitMeta;
import static org.projectnessie.services.spi.TreeService.MAX_COMMIT_LOG_ENTRIES;
import static org.projectnessie.versioned.RequestMeta.API_READ;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.List;
import java.util.Locale;
import org.projectnessie.api.v2.http.HttpTreeApi;
import org.projectnessie.api.v2.params.CommitLogParams;
import org.projectnessie.api.v2.params.DiffParams;
import org.projectnessie.api.v2.params.EntriesParams;
import org.projectnessie.api.v2.params.GetReferenceParams;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.api.v2.params.ReferenceHistoryParams;
import org.projectnessie.api.v2.params.ReferencesParams;
import org.projectnessie.api.v2.params.Transplant;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ImmutableEntriesResponse;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutableReferencesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.ContentApiImpl;
import org.projectnessie.services.impl.DiffApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.spi.ConfigService;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedCountingResponseHandler;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the tree-API. */
@RequestScoped
@Path("api/v2/trees")
public class RestV2TreeResource implements HttpTreeApi {

  private final ConfigService configService;
  private final TreeService treeService;
  private final ContentService contentService;
  private final DiffService diffService;
  private final HttpHeaders httpHeaders;

  // Mandated by CDI 2.0
  public RestV2TreeResource() {
    this(null, null, null, null, null);
  }

  @Inject
  public RestV2TreeResource(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext,
      HttpHeaders httpHeaders) {
    this.configService = new ConfigApiImpl(config, store, authorizer, accessContext, NESSIE_V2);
    this.treeService = new TreeApiImpl(config, store, authorizer, accessContext, NESSIE_V2);
    this.contentService = new ContentApiImpl(config, store, authorizer, accessContext, NESSIE_V2);
    this.diffService = new DiffApiImpl(config, store, authorizer, accessContext, NESSIE_V2);
    this.httpHeaders = httpHeaders;
  }

  private ParsedReference parseRefPathString(String refPathString) {
    return parseRefPathString(refPathString, Reference.ReferenceType.BRANCH);
  }

  private ParsedReference parseRefPathString(
      String refPathString, Reference.ReferenceType referenceType) {
    return resolveReferencePathElement(
        refPathString, referenceType, () -> configService.getConfig().getDefaultBranch());
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
            new PagedCountingResponseHandler<>(maxRecords) {
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
  public SingleReferenceResponse createReference(String name, String type, Reference reference)
      throws NessieNotFoundException, NessieConflictException {

    Reference.ReferenceType referenceType = parseReferenceType(type);
    checkArgument(referenceType != null, "Mandatory reference type missing");

    String fromRefName = null;
    String fromHash = null;
    if (reference != null) {
      fromRefName = reference.getName();
      fromHash = reference.getHash();
    }

    Reference created = tree().createReference(name, referenceType, fromHash, fromRefName);
    return SingleReferenceResponse.builder().reference(created).build();
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse getReferenceByName(GetReferenceParams params)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(params.getRef());
    checkArgument(
        reference.hashWithRelativeSpec() == null,
        "Hashes are not allowed when fetching a reference by name");
    return SingleReferenceResponse.builder()
        .reference(tree().getReferenceByName(reference.name(), params.fetchOption()))
        .build();
  }

  @JsonView(Views.V2.class)
  @Override
  public ReferenceHistoryResponse getReferenceHistory(ReferenceHistoryParams params)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(params.getRef());
    checkArgument(
        reference.hashWithRelativeSpec() == null,
        "Hashes are not allowed when fetching a reference history");
    return tree().getReferenceHistory(reference.name(), params.headCommitsToScan());
  }

  @JsonView(Views.V2.class)
  @Override
  public EntriesResponse getEntries(String ref, EntriesParams params)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(ref);
    Integer maxRecords = params.maxRecords();
    ImmutableEntriesResponse.Builder builder = EntriesResponse.builder();
    return tree()
        .getEntries(
            reference.name(),
            reference.hashWithRelativeSpec(),
            null,
            params.filter(),
            params.pageToken(),
            params.withContent(),
            new PagedCountingResponseHandler<>(maxRecords) {
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
            h -> builder.effectiveReference(toReference(h)),
            params.minKey(),
            params.maxKey(),
            params.prefixKey(),
            params.getRequestedKeys());
  }

  @JsonView(Views.V2.class)
  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(ref);
    Integer maxRecords = params.maxRecords();
    return tree()
        .getCommitLog(
            reference.name(),
            params.fetchOption(),
            params.startHash(),
            reference.hashWithRelativeSpec(),
            params.filter(),
            params.pageToken(),
            new PagedCountingResponseHandler<>(maxRecords, MAX_COMMIT_LOG_ENTRIES) {
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
    ParsedReference from = parseRefPathString(params.getFromRef());
    ParsedReference to = parseRefPathString(params.getToRef());
    ImmutableDiffResponse.Builder builder = DiffResponse.builder();
    return diff()
        .getDiff(
            from.name(),
            from.hashWithRelativeSpec(),
            to.name(),
            to.hashWithRelativeSpec(),
            params.pageToken(),
            new PagedCountingResponseHandler<>(maxRecords) {
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
            h -> builder.effectiveToReference(toReference(h)),
            params.minKey(),
            params.maxKey(),
            params.prefixKey(),
            params.getRequestedKeys(),
            params.getFilter());
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse assignReference(String type, String ref, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    ParsedReference reference = parseRefPathString(ref, parseReferenceType(type));
    Reference updated =
        tree()
            .assignReference(
                reference.type(), reference.name(), reference.hashWithRelativeSpec(), assignTo);
    return SingleReferenceResponse.builder().reference(updated).build();
  }

  @JsonView(Views.V2.class)
  @Override
  public SingleReferenceResponse deleteReference(String type, String ref)
      throws NessieConflictException, NessieNotFoundException {
    ParsedReference reference = parseRefPathString(ref, parseReferenceType(type));
    Reference deleted =
        tree()
            .deleteReference(reference.type(), reference.name(), reference.hashWithRelativeSpec());
    return SingleReferenceResponse.builder().reference(deleted).build();
  }

  private static Reference.ReferenceType parseReferenceType(String type) {
    if (type == null) {
      return null;
    }
    return Reference.ReferenceType.valueOf(type.toUpperCase(Locale.ROOT));
  }

  @JsonView(Views.V2.class)
  @Override
  public ContentResponse getContent(
      ContentKey key, String ref, boolean withDocumentation, boolean forWrite)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(ref);
    return content()
        .getContent(
            key, reference.name(), reference.hashWithRelativeSpec(), withDocumentation, API_READ);
  }

  @JsonView(Views.V2.class)
  @Override
  public GetMultipleContentsResponse getSeveralContents(
      String ref, List<String> keys, boolean withDocumentation, boolean forWrite)
      throws NessieNotFoundException {
    ImmutableGetMultipleContentsRequest.Builder request = GetMultipleContentsRequest.builder();
    keys.forEach(k -> request.addRequestedKeys(ContentKey.fromPathString(k)));
    return getMultipleContents(ref, request.build(), withDocumentation, forWrite);
  }

  @JsonView(Views.V2.class)
  @Override
  public GetMultipleContentsResponse getMultipleContents(
      String ref, GetMultipleContentsRequest request, boolean withDocumentation, boolean forWrite)
      throws NessieNotFoundException {
    ParsedReference reference = parseRefPathString(ref);
    return content()
        .getMultipleContents(
            reference.name(),
            reference.hashWithRelativeSpec(),
            request.getRequestedKeys(),
            withDocumentation,
            API_READ);
  }

  @JsonView(Views.V2.class)
  @Override
  public MergeResponse transplantCommitsIntoBranch(String branch, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    ParsedReference ref = parseRefPathString(branch);

    String msg = transplant.getMessage();
    CommitMeta meta = commitMeta(CommitMeta.builder().message(msg == null ? "" : msg)).build();

    return tree()
        .transplantCommitsIntoBranch(
            ref.name(),
            ref.hashWithRelativeSpec(),
            meta,
            transplant.getHashesToTransplant(),
            transplant.getFromRefName(),
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
    ParsedReference ref = parseRefPathString(branch);

    @SuppressWarnings("deprecation")
    String msg = merge.getMessage();

    ImmutableCommitMeta.Builder meta = CommitMeta.builder();
    CommitMeta commitMeta = merge.getCommitMeta();
    if (commitMeta != null) {
      meta.from(commitMeta);
      if (commitMeta.getMessage().isEmpty() && msg != null) {
        meta.message(msg);
      }
    } else {
      meta.message(msg == null ? "" : msg);
    }
    commitMeta(meta);

    return tree()
        .mergeRefIntoBranch(
            ref.name(),
            ref.hashWithRelativeSpec(),
            merge.getFromRefName(),
            merge.getFromHash(),
            meta.build(),
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
    ImmutableOperations.Builder ops =
        ImmutableOperations.builder()
            .from(operations)
            .commitMeta(commitMeta(CommitMeta.builder().from(operations.getCommitMeta())).build());

    ParsedReference ref = parseRefPathString(branch);
    return tree()
        .commitMultipleOperations(ref.name(), ref.hashWithRelativeSpec(), ops.build(), API_WRITE);
  }

  CommitMeta.Builder commitMeta(CommitMeta.Builder commitMeta) {
    return updateCommitMeta(commitMeta, httpHeaders);
  }
}
