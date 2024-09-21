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
package org.projectnessie.versioned;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.paging.PaginationIterator;

public class ObservingVersionStore implements VersionStore {
  private final VersionStore delegate;

  private static final String PREFIX = "nessie.versionstore.request";

  private static final String TAG_REF = "ref";
  private static final String TAG_BRANCH = "branch";
  private static final String TAG_HASH = "hash";
  private static final String TAG_EXPECTED_HASH = "expected-hash";
  private static final String TAG_TARGET_HASH = "target-hash";
  private static final String TAG_FROM = "from";
  private static final String TAG_TO = "to";

  public ObservingVersionStore(VersionStore delegate) {
    this.delegate = delegate;
  }

  @Nonnull
  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public RepositoryInformation getRepositoryInformation() {
    return delegate.getRepositoryInformation();
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Hash hashOnReference(
      @SpanAttribute(TAG_REF) NamedRef namedReference,
      @SpanAttribute(TAG_HASH) Optional<Hash> hashOnReference,
      List<RelativeCommitSpec> relativeLookups)
      throws ReferenceNotFoundException {
    return delegate.hashOnReference(namedReference, hashOnReference, relativeLookups);
  }

  @Nonnull
  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public CommitResult commit(
      @SpanAttribute(TAG_BRANCH) @Nonnull BranchName branch,
      @SpanAttribute(TAG_HASH) @Nonnull Optional<Hash> referenceHash,
      @Nonnull CommitMeta metadata,
      @Nonnull List<Operation> operations,
      @Nonnull CommitValidator validator,
      @Nonnull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.commit(branch, referenceHash, metadata, operations, validator, addedContents);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public TransplantResult transplant(TransplantOp transplantOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.transplant(transplantOp);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public MergeResult merge(MergeOp mergeOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.merge(mergeOp);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceAssignedResult assign(
      @SpanAttribute(TAG_REF) NamedRef ref,
      @SpanAttribute(TAG_EXPECTED_HASH) Hash expectedHash,
      @SpanAttribute(TAG_TARGET_HASH) Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.assign(ref, expectedHash, targetHash);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceCreatedResult create(
      @SpanAttribute(TAG_REF) NamedRef ref,
      @SpanAttribute(TAG_TARGET_HASH) Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return delegate.create(ref, targetHash);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceDeletedResult delete(
      @SpanAttribute(TAG_REF) NamedRef ref, @SpanAttribute(TAG_HASH) Hash hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.delete(ref, hash);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceInfo<CommitMeta> getNamedRef(
      @SpanAttribute(TAG_REF) String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return delegate.getNamedRef(ref, params);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceHistory getReferenceHistory(String refName, Integer headCommitsToScan)
      throws ReferenceNotFoundException {
    return delegate.getReferenceHistory(refName, headCommitsToScan);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    return delegate.getNamedRefs(params, pagingToken);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public PaginationIterator<Commit> getCommits(
      @SpanAttribute(TAG_REF) Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return delegate.getCommits(ref, fetchAdditionalInfo);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public PaginationIterator<KeyEntry> getKeys(
      @SpanAttribute(TAG_REF) Ref ref,
      String pagingToken,
      boolean withContent,
      KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    return delegate.getKeys(ref, pagingToken, withContent, keyRestrictions);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public List<IdentifiedContentKey> getIdentifiedKeys(
      @SpanAttribute(TAG_REF) Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return delegate.getIdentifiedKeys(ref, keys);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ContentResult getValue(
      @SpanAttribute(TAG_REF) Ref ref, ContentKey key, boolean returnNotFound)
      throws ReferenceNotFoundException {
    return delegate.getValue(ref, key, returnNotFound);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Map<ContentKey, ContentResult> getValues(
      @SpanAttribute(TAG_REF) Ref ref, Collection<ContentKey> keys, boolean returnNotFound)
      throws ReferenceNotFoundException {
    return delegate.getValues(ref, keys, returnNotFound);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public PaginationIterator<Diff> getDiffs(
      @SpanAttribute(TAG_FROM) Ref from,
      @SpanAttribute(TAG_TO) Ref to,
      String pagingToken,
      KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    return delegate.getDiffs(from, to, pagingToken, keyRestrictions);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public List<RepositoryConfig> getRepositoryConfig(
      Set<RepositoryConfig.Type> repositoryConfigTypes) {
    return delegate.getRepositoryConfig(repositoryConfigTypes);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public RepositoryConfig updateRepositoryConfig(RepositoryConfig repositoryConfig)
      throws ReferenceConflictException {
    return delegate.updateRepositoryConfig(repositoryConfig);
  }
}
