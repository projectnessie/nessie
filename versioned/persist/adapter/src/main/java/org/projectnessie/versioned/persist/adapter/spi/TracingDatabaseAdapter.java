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

import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitAttempt;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ContentIdWithType;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;

public final class TracingDatabaseAdapter implements DatabaseAdapter {
  private static final String TAG_COUNT = "count";
  private static final String TAG_REF = "ref";
  private static final String TAG_HASH = "hash";
  private static final String TAG_FROM = "from";
  private static final String TAG_TO = "to";

  private final DatabaseAdapter delegate;

  public TracingDatabaseAdapter(DatabaseAdapter delegate) {
    this.delegate = delegate;
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
  public Map<Key, ContentAndState<ByteString>> values(
      Hash commit, Collection<Key> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    try (Traced ignore =
        trace("values").tag(TAG_HASH, commit.asString()).tag(TAG_COUNT, keys.size())) {
      return delegate.values(commit, keys, keyFilter);
    }
  }

  @Override
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    try (Traced ignore = trace("commitLog.stream").tag(TAG_HASH, offset.asString())) {
      return delegate.commitLog(offset);
    }
  }

  @Override
  public Stream<KeyWithType> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("keys.stream").tag(TAG_HASH, commit.asString())) {
      return delegate.keys(commit, keyFilter);
    }
  }

  @Override
  public Hash commit(CommitAttempt commitAttempt)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try (Traced ignore =
        trace("commit").tag(TAG_REF, commitAttempt.getCommitToBranch().getName())) {
      return delegate.commit(commitAttempt);
    }
  }

  @Override
  public Hash transplant(
      BranchName targetBranch,
      Optional<Hash> expectedHead,
      List<Hash> sequenceToTransplant,
      Function<ByteString, ByteString> updateCommitMetadata)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore = trace("transplant").tag(TAG_REF, targetBranch.getName())) {
      return delegate.transplant(
          targetBranch, expectedHead, sequenceToTransplant, updateCommitMetadata);
    }
  }

  @Override
  public Hash merge(
      Hash from,
      BranchName toBranch,
      Optional<Hash> expectedHead,
      Function<ByteString, ByteString> updateCommitMetadata)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore =
        trace("merge").tag(TAG_REF, toBranch.getName()).tag(TAG_HASH, from.asString())) {
      return delegate.merge(from, toBranch, expectedHead, updateCommitMetadata);
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
  public Hash create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    try (Traced ignore =
        trace("create").tag(TAG_REF, ref.getName()).tag(TAG_HASH, target.asString())) {
      return delegate.create(ref, target);
    }
  }

  @Override
  public void delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore = trace("delete").tag(TAG_REF, reference.getName())) {
      delegate.delete(reference, expectedHead);
    }
  }

  @Override
  public void assign(NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try (Traced ignore =
        trace("assign").tag(TAG_HASH, assignTo.asString()).tag(TAG_REF, assignee.getName())) {
      delegate.assign(assignee, expectedHead, assignTo);
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
  public Stream<ContentIdWithType> globalKeys(ToIntFunction<ByteString> contentTypeExtractor) {
    try (Traced ignore = trace("globalKeys.stream")) {
      return delegate.globalKeys(contentTypeExtractor);
    }
  }

  @Override
  public Stream<ContentIdAndBytes> globalContent(
      Set<ContentId> keys, ToIntFunction<ByteString> contentTypeExtractor) {
    try (Traced ignore = trace("globalContent.stream").tag(TAG_COUNT, keys.size())) {
      return delegate.globalContent(keys, contentTypeExtractor);
    }
  }

  @Override
  public Optional<ContentIdAndBytes> globalContent(
      ContentId contentId, ToIntFunction<ByteString> contentTypeExtractor) {
    try (Traced ignore = trace("globalContent")) {
      return delegate.globalContent(contentId, contentTypeExtractor);
    }
  }

  @Override
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    try (Traced ignore =
        trace("refLog.stream").tag(TAG_HASH, offset != null ? offset.asString() : "HEAD")) {
      return delegate.refLog(offset);
    }
  }
}
