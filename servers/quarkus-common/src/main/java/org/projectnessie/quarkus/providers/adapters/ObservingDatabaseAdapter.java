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
package org.projectnessie.quarkus.providers.adapters;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.quarkus.providers.NotObserved;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.CommitParams;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;

@Singleton
public class ObservingDatabaseAdapter implements DatabaseAdapter {
  private final DatabaseAdapter delegate;

  private static final String PREFIX = "nessie.storage.adapter";

  public ObservingDatabaseAdapter(@NotObserved DatabaseAdapter delegate) {
    this.delegate = delegate;
  }

  @Override
  public DatabaseAdapterConfig getConfig() {
    return delegate.getConfig();
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void initializeRepo(String defaultBranchName) {
    delegate.initializeRepo(defaultBranchName);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void eraseRepo() {
    delegate.eraseRepo();
  }

  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return delegate.hashOnReference(namedReference, hashOnReference);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Map<ContentKey, ContentAndState> values(
      Hash commit, Collection<ContentKey> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return delegate.values(commit, keys, keyFilter);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @MustBeClosed
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    return delegate.commitLog(offset);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @MustBeClosed
  public Stream<CommitLogEntry> fetchCommitLogEntries(Stream<Hash> hashes) {
    return delegate.fetchCommitLogEntries(hashes);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @MustBeClosed
  public Stream<KeyListEntry> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return delegate.keys(commit, keyFilter);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public CommitResult<CommitLogEntry> commit(CommitParams commitParams)
      throws ReferenceConflictException, ReferenceNotFoundException {
    return delegate.commit(commitParams);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public MergeResult<CommitLogEntry> transplant(TransplantParams transplantParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.transplant(transplantParams);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public MergeResult<CommitLogEntry> merge(MergeParams mergeParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.merge(mergeParams);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceInfo<ByteString> namedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return delegate.namedRef(ref, params);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @MustBeClosed
  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return delegate.namedRefs(params);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceCreatedResult create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    return delegate.create(ref, target);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceDeletedResult delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.delete(reference, expectedHead);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public ReferenceAssignedResult assign(
      NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return delegate.assign(assignee, expectedHead, assignTo);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @MustBeClosed
  public Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return delegate.diff(from, to, keyFilter);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public RepoDescription fetchRepositoryDescription() {
    return delegate.fetchRepositoryDescription();
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void updateRepositoryDescription(Function<RepoDescription, RepoDescription> updater)
      throws ReferenceConflictException {
    delegate.updateRepositoryDescription(updater);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Optional<ContentIdAndBytes> globalContent(ContentId contentId) {
    return delegate.globalContent(contentId);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public Map<String, Map<String, String>> repoMaintenance(
      RepoMaintenanceParams repoMaintenanceParams) {
    return delegate.repoMaintenance(repoMaintenanceParams);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  @MustBeClosed
  public Stream<CommitLogEntry> scanAllCommitLogEntries() {
    return delegate.scanAllCommitLogEntries();
  }

  @Override
  @VisibleForTesting
  public void assertCleanStateForTests() {
    delegate.assertCleanStateForTests();
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void writeMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceConflictException {
    delegate.writeMultipleCommits(commitLogEntries);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public void updateMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceNotFoundException {
    delegate.updateMultipleCommits(commitLogEntries);
  }

  @WithSpan
  @Override
  @Counted(PREFIX)
  @Timed(value = PREFIX, histogram = true)
  public CommitLogEntry rebuildKeyList(
      CommitLogEntry entry,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    return delegate.rebuildKeyList(entry, inMemoryCommits);
  }
}
