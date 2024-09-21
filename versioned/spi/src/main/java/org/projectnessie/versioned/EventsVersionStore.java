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

import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.paging.PaginationIterator;

/**
 * A {@link VersionStore} wrapper that publishes results when a method is called that changes the
 * catalog state.
 */
public class EventsVersionStore implements VersionStore {

  private final VersionStore delegate;
  private final Consumer<Result> resultSink;

  /**
   * Takes the {@link VersionStore} instance to enrich with events.
   *
   * @param delegate backing/delegate {@link VersionStore}.
   * @param resultSink a consumer for results.
   */
  public EventsVersionStore(VersionStore delegate, Consumer<Result> resultSink) {
    this.delegate = delegate;
    this.resultSink = resultSink;
  }

  @Override
  public CommitResult commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull CommitMeta metadata,
      @Nonnull List<Operation> operations,
      @Nonnull CommitValidator validator,
      @Nonnull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException {
    CommitResult result =
        delegate.commit(branch, referenceHash, metadata, operations, validator, addedContents);
    resultSink.accept(result);
    return result;
  }

  @Override
  public TransplantResult transplant(TransplantOp transplantOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    TransplantResult result = delegate.transplant(transplantOp);
    if (result.wasApplied()) {
      resultSink.accept(result);
    }
    return result;
  }

  @Override
  public MergeResult merge(MergeOp mergeOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    MergeResult result = delegate.merge(mergeOp);
    if (result.wasApplied()) {
      resultSink.accept(result);
    }
    return result;
  }

  @Override
  public ReferenceAssignedResult assign(NamedRef ref, Hash expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    ReferenceAssignedResult result = delegate.assign(ref, expectedHash, targetHash);
    resultSink.accept(result);
    return result;
  }

  @Override
  public ReferenceCreatedResult create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    // FIXME half-created refs with new storage model
    ReferenceCreatedResult result = delegate.create(ref, targetHash);
    resultSink.accept(result);
    return result;
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef ref, Hash hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    ReferenceDeletedResult result = delegate.delete(ref, hash);
    resultSink.accept(result);
    return result;
  }

  @Nonnull
  @Override
  public RepositoryInformation getRepositoryInformation() {
    return delegate.getRepositoryInformation();
  }

  @Override
  public Hash hashOnReference(
      NamedRef namedReference,
      Optional<Hash> hashOnReference,
      List<RelativeCommitSpec> relativeLookups)
      throws ReferenceNotFoundException {
    return delegate.hashOnReference(namedReference, hashOnReference, relativeLookups);
  }

  @Nonnull
  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return delegate.getNamedRef(ref, params);
  }

  @Override
  public ReferenceHistory getReferenceHistory(String refName, Integer headCommitsToScan)
      throws ReferenceNotFoundException {
    return delegate.getReferenceHistory(refName, headCommitsToScan);
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    return delegate.getNamedRefs(params, pagingToken);
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    return delegate.getCommits(ref, fetchAdditionalInfo);
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(
      Ref ref, String pagingToken, boolean withContent, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    return delegate.getKeys(ref, pagingToken, withContent, keyRestrictions);
  }

  @Override
  public List<IdentifiedContentKey> getIdentifiedKeys(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    return delegate.getIdentifiedKeys(ref, keys);
  }

  @Override
  public ContentResult getValue(Ref ref, ContentKey key, boolean returnNotFound)
      throws ReferenceNotFoundException {
    return delegate.getValue(ref, key, returnNotFound);
  }

  @Override
  public Map<ContentKey, ContentResult> getValues(
      Ref ref, Collection<ContentKey> keys, boolean returnNotFound)
      throws ReferenceNotFoundException {
    return delegate.getValues(ref, keys, returnNotFound);
  }

  @Override
  public PaginationIterator<Diff> getDiffs(
      Ref from, Ref to, String pagingToken, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    return delegate.getDiffs(from, to, pagingToken, keyRestrictions);
  }

  @Override
  public List<RepositoryConfig> getRepositoryConfig(
      Set<RepositoryConfig.Type> repositoryConfigTypes) {
    return delegate.getRepositoryConfig(repositoryConfigTypes);
  }

  @Override
  public RepositoryConfig updateRepositoryConfig(RepositoryConfig repositoryConfig)
      throws ReferenceConflictException {
    return delegate.updateRepositoryConfig(repositoryConfig);
  }
}
