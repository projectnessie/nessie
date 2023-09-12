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
package org.projectnessie.versioned.persist.adapter.spi;

import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
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

@SuppressWarnings("MustBeClosedChecker")
public final class TracingDatabaseAdapter implements DatabaseAdapter {
  private static final String TAG_COUNT = "count";
  private static final String TAG_REF = "ref";
  private static final String TAG_HASH = "hash";
  private static final String TAG_FROM = "from";
  private static final String TAG_TO = "to";
  private static final String TAG_CONTENT_ID = "cid";

  private final DatabaseAdapter delegate;

  public TracingDatabaseAdapter(DatabaseAdapter delegate) {
    this.delegate = delegate;
  }

  @Override
  public DatabaseAdapterConfig getConfig() {
    return delegate.getConfig();
  }

  @Override
  public void initializeRepo(String defaultBranchName) {
    try (Traced ignore = trace("initializeRepo").tag(TAG_REF, defaultBranchName)) {
      delegate.initializeRepo(defaultBranchName);
    }
  }

  @Override
  public void eraseRepo() {
    try (Traced ignore = trace("eraseRepo")) {
      delegate.eraseRepo();
    }
  }

  @Override
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("hashOnReference").tag(TAG_REF, namedReference.getName())) {
      return delegate.hashOnReference(namedReference, hashOnReference);
    }
  }

  @Override
  public Map<ContentKey, ContentAndState> values(
      Hash commit, Collection<ContentKey> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    try (Traced ignore =
        trace("values").tag(TAG_HASH, commit.asString()).tag(TAG_COUNT, keys.size())) {
      return delegate.values(commit, keys, keyFilter);
    }
  }

  @Override
  public Stream<CommitLogEntry> fetchCommitLogEntries(Stream<Hash> hashes) {
    try (Traced ignore = trace("fetchCommitLogEntries.stream")) {
      return delegate.fetchCommitLogEntries(hashes);
    }
  }

  @Override
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    try (Traced ignore = trace("commitLog.stream").tag(TAG_HASH, offset.asString())) {
      return delegate.commitLog(offset);
    }
  }

  @Override
  public Stream<KeyListEntry> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("keys.stream").tag(TAG_HASH, commit.asString())) {
      return delegate.keys(commit, keyFilter);
    }
  }

  @Override
  public CommitResult<CommitLogEntry> commit(CommitParams commitParams)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try (Traced ignore = trace("commit").tag(TAG_REF, commitParams.getToBranch().getName())) {
      return delegate.commit(commitParams);
    }
  }

  @Override
  public MergeResult<CommitLogEntry> transplant(TransplantParams transplantParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore =
        trace("transplant").tag(TAG_REF, transplantParams.getToBranch().getName())) {
      return delegate.transplant(transplantParams);
    }
  }

  @Override
  public MergeResult<CommitLogEntry> merge(MergeParams mergeParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore =
        trace("merge")
            .tag(TAG_REF, mergeParams.getToBranch().getName())
            .tag(TAG_HASH, mergeParams.getMergeFromHash().asString())) {
      return delegate.merge(mergeParams);
    }
  }

  @Override
  public ReferenceInfo<ByteString> namedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("namedRef").tag(TAG_REF, ref)) {
      return delegate.namedRef(ref, params);
    }
  }

  @Override
  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("namedRefs.stream")) {
      return delegate.namedRefs(params);
    }
  }

  @Override
  public ReferenceCreatedResult create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    try (Traced ignore =
        trace("create").tag(TAG_REF, ref.getName()).tag(TAG_HASH, target.asString())) {
      return delegate.create(ref, target);
    }
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore = trace("delete").tag(TAG_REF, reference.getName())) {
      return delegate.delete(reference, expectedHead);
    }
  }

  @Override
  public ReferenceAssignedResult assign(
      NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore =
        trace("assign").tag(TAG_HASH, assignTo.asString()).tag(TAG_REF, assignee.getName())) {
      return delegate.assign(assignee, expectedHead, assignTo);
    }
  }

  @Override
  public Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    try (Traced ignore =
        trace("diff.stream").tag(TAG_FROM, from.asString()).tag(TAG_TO, to.asString())) {
      return delegate.diff(from, to, keyFilter);
    }
  }

  @Override
  public RepoDescription fetchRepositoryDescription() {
    try (Traced ignore = trace("fetchRepositoryDescription")) {
      return delegate.fetchRepositoryDescription();
    }
  }

  @Override
  public void updateRepositoryDescription(Function<RepoDescription, RepoDescription> updater)
      throws ReferenceConflictException {
    try (Traced ignore = trace("updateRepositoryDescription")) {
      delegate.updateRepositoryDescription(updater);
    }
  }

  @Override
  public Optional<ContentIdAndBytes> globalContent(ContentId contentId) {
    try (Traced ignore = trace("globalContent").tag(TAG_CONTENT_ID, contentId.getId())) {
      return delegate.globalContent(contentId);
    }
  }

  @Override
  public Map<String, Map<String, String>> repoMaintenance(
      RepoMaintenanceParams repoMaintenanceParams) {
    try (Traced ignore = trace("repoMaintenance")) {
      return delegate.repoMaintenance(repoMaintenanceParams);
    }
  }

  @Override
  public Stream<CommitLogEntry> scanAllCommitLogEntries() {
    try (Traced ignore = trace("scanAllCommitLogEntries")) {
      return delegate.scanAllCommitLogEntries();
    }
  }

  @Override
  public void assertCleanStateForTests() {
    delegate.assertCleanStateForTests();
  }

  @Override
  public void writeMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceConflictException {
    try (Traced ignore = trace("writeMultipleCommits").tag(TAG_COUNT, commitLogEntries.size())) {
      delegate.writeMultipleCommits(commitLogEntries);
    }
  }

  @Override
  public void updateMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("updateMultipleCommits").tag(TAG_COUNT, commitLogEntries.size())) {
      delegate.updateMultipleCommits(commitLogEntries);
    }
  }

  @Override
  public CommitLogEntry rebuildKeyList(
      CommitLogEntry entry,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("rebuildKeyList").tag(TAG_HASH, entry.getHash().asString())) {
      return delegate.rebuildKeyList(entry, inMemoryCommits);
    }
  }
}
