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

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
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
import org.projectnessie.model.Content;
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
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.model.SingleReferenceResponse;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ContentApiImplWithAuthorization;
import org.projectnessie.services.impl.DiffApiImplWithAuthorization;
import org.projectnessie.services.impl.TreeApiImplWithAuthorization;
import org.projectnessie.services.spi.ContentService;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.TreeService;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the tree-API. */
@RequestScoped
public class RestV2TreeResource implements HttpTreeApi {

  private static final String DEFAULT_REF_IN_PATH = "-";

  private final ServerConfig config;
  private final VersionStore store;
  private final Authorizer authorizer;

  @Context SecurityContext securityContext;

  // Mandated by CDI 2.0
  public RestV2TreeResource() {
    this(null, null, null);
  }

  @Inject
  public RestV2TreeResource(ServerConfig config, VersionStore store, Authorizer authorizer) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
  }

  private Reference resolveRef(String refPathString) {
    if (DEFAULT_REF_IN_PATH.equals(refPathString)) {
      return Branch.of(config.getDefaultBranch(), null);
    }

    return resolveRef(refPathString, Reference.ReferenceType.BRANCH);
  }

  private Reference resolveRef(String refPathString, Reference.ReferenceType type) {
    return Reference.fromPathString(refPathString, type);
  }

  private TreeService tree() {
    return new TreeApiImplWithAuthorization(
        config,
        store,
        authorizer,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  private DiffService diff() {
    return new DiffApiImplWithAuthorization(
        config,
        store,
        authorizer,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  private ContentService content() {
    return new ContentApiImplWithAuthorization(
        config,
        store,
        authorizer,
        securityContext == null ? null : securityContext.getUserPrincipal());
  }

  @Override
  public ReferencesResponse getAllReferences(ReferencesParams params) {
    return tree().getAllReferences(params.fetchOption(), params.filter());
  }

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

  @Override
  public SingleReferenceResponse getReferenceByName(GetReferenceParams params)
      throws NessieNotFoundException {
    Reference reference = resolveRef(params.getRef());
    return SingleReferenceResponse.builder()
        .reference(tree().getReferenceByName(reference.getName(), params.fetchOption()))
        .build();
  }

  @Override
  public EntriesResponse getEntries(String ref, EntriesParams params)
      throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    return tree().getEntries(reference.getName(), reference.getHash(), null, params.filter());
  }

  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    return tree()
        .getCommitLog(
            reference.getName(),
            params.fetchOption(),
            params.startHash(),
            reference.getHash(),
            params.filter(),
            params.maxRecords(),
            params.pageToken());
  }

  @Override
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    Reference from = resolveRef(params.getFromRef());
    Reference to = resolveRef(params.getToRef());
    return diff().getDiff(from.getName(), from.getHash(), to.getName(), to.getHash());
  }

  @Override
  public SingleReferenceResponse assignReference(
      Reference.ReferenceType type, String ref, Reference assignTo)
      throws NessieNotFoundException, NessieConflictException {
    Reference reference = resolveRef(ref, type);
    tree().assignReference(type, reference.getName(), reference.getHash(), assignTo);
    return SingleReferenceResponse.builder().reference(reference).build();
  }

  @Override
  public SingleReferenceResponse deleteReference(Reference.ReferenceType type, String ref)
      throws NessieConflictException, NessieNotFoundException {
    Reference reference = resolveRef(ref, type);
    tree().deleteReference(type, reference.getName(), reference.getHash());
    return SingleReferenceResponse.builder().reference(reference).build();
  }

  @Override
  public ContentResponse getContent(ContentKey key, String ref) throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    Content content = content().getContent(key, reference.getName(), reference.getHash());
    return ContentResponse.builder().content(content).build();
  }

  @Override
  public GetMultipleContentsResponse getMultipleContents(
      String ref, GetMultipleContentsRequest request) throws NessieNotFoundException {
    Reference reference = resolveRef(ref);
    return content()
        .getMultipleContents(reference.getName(), reference.getHash(), request.getRequestedKeys());
  }

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
            merge.getKeyMergeModes(),
            merge.getDefaultKeyMergeMode(),
            merge.isDryRun(),
            merge.isFetchAdditionalInfo(),
            merge.isReturnConflictAsResult());
  }

  @Override
  public CommitResponse commitMultipleOperations(String branch, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    Reference ref = resolveRef(branch);
    Branch head = tree().commitMultipleOperations(ref.getName(), ref.getHash(), operations);
    return CommitResponse.builder().targetBranch(head).build();
  }
}
