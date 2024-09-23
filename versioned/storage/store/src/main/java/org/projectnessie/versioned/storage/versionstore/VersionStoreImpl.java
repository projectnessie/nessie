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
package org.projectnessie.versioned.storage.versionstore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.model.IdentifiedContentKey.identifiedContentKeyFromContent;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.ContentResult.contentResult;
import static org.projectnessie.versioned.ReferenceHistory.ReferenceHistoryElement.referenceHistoryElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.DiffQuery.diffQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.consistencyLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.fromString;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.versionstore.BaseCommitHelper.committingOperation;
import static org.projectnessie.versioned.storage.versionstore.BaseCommitHelper.dryRunCommitterSupplier;
import static org.projectnessie.versioned.storage.versionstore.KeyRanges.keyRanges;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.NO_ANCESTOR;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.asBranchName;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.asTagName;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.namedRefToRefName;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.objectNotFound;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceAlreadyExists;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceConflictException;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceNotFound;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceToNamedRef;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.verifyExpectedHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.CONTENT_DISCRIMINATOR;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKeyMin;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKeyNoVariant;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;
import static org.projectnessie.versioned.store.DefaultStoreWorker.contentTypeForPayload;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import org.projectnessie.model.CommitConsistency;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.ImmutableReferenceHistory;
import org.projectnessie.versioned.ImmutableReferenceInfo;
import org.projectnessie.versioned.ImmutableRepositoryInformation;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeTransplantResultBase;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceHistory;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceInfo.CommitsAheadBehind;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RelativeCommitSpec;
import org.projectnessie.versioned.RepositoryInformation;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.TransplantResult;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.FilteringPaginationIterator;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.ConsistencyLogic;
import org.projectnessie.versioned.storage.common.logic.DiffEntry;
import org.projectnessie.versioned.storage.common.logic.DiffPagedResult;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.PagingToken;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.versionstore.BaseCommitHelper.CommitterSupplier;

public class VersionStoreImpl implements VersionStore {

  public static final int GET_KEYS_CONTENT_BATCH_SIZE = 50;
  private final Persist persist;

  @SuppressWarnings("unused")
  public VersionStoreImpl() {
    this(null);
  }

  public VersionStoreImpl(Persist persist) {
    this.persist = persist;
  }

  @Nonnull
  @Override
  public RepositoryInformation getRepositoryInformation() {
    ImmutableRepositoryInformation.Builder repoInfo =
        ImmutableRepositoryInformation.builder().noAncestorHash(noAncestorHash().asString());
    RepositoryDescription desc = repositoryLogic(persist).fetchRepositoryDescription();
    if (desc != null) {
      repoInfo
          .repositoryCreationTimestamp(desc.repositoryCreatedTime())
          .oldestPossibleCommitTimestamp(desc.oldestPossibleCommitTime())
          .defaultBranch(desc.defaultBranchName());
    }
    return repoInfo.build();
  }

  @Nonnull
  @Override
  public Hash noAncestorHash() {
    return RefMapping.NO_ANCESTOR;
  }

  @Override
  public Hash hashOnReference(
      NamedRef namedRef, Optional<Hash> hashOnReference, List<RelativeCommitSpec> relativeLookups)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head;
    if (DetachedRef.INSTANCE.equals(namedRef)) {
      checkArgument(hashOnReference.isPresent(), "Must supply 'hashOnReference' for DETACHED");
      try {
        head = commitLogic(persist).fetchCommit(hashToObjId(hashOnReference.get()));
      } catch (ObjNotFoundException e) {
        throw referenceNotFound(e);
      }
    } else {
      head = refMapping.resolveNamedRefHead(namedRef);
    }

    CommitObj commit = refMapping.commitInChain(namedRef, head, hashOnReference, relativeLookups);
    return commit != null ? objIdToHash(commit.id()) : NO_ANCESTOR;
  }

  @Override
  public ReferenceCreatedResult create(NamedRef namedRef, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    try {
      ObjId objId;
      if (targetHash.isPresent()) {
        Hash hash = targetHash.get();
        objId = hashToObjId(hash);
        if (!hash.equals(RefMapping.NO_ANCESTOR) && persist.fetchObjType(objId) != COMMIT) {
          throw RefMapping.hashNotFound(hash);
        }
      } else {
        objId = EMPTY_OBJ_ID;
      }

      String mustNotExist =
          namedRef instanceof TagName
              ? asBranchName(namedRef.getName())
              : asTagName(namedRef.getName());
      try {
        referenceLogic.getReferenceForUpdate(mustNotExist);
        // A tag with the same name as the branch being created (or a branch with the same name
        // as the tag being created) already exists.
        throw referenceAlreadyExists(namedRef);
      } catch (RefNotFoundException good) {
        Reference reference =
            referenceLogic.createReference(namedRefToRefName(namedRef), objId, null);
        return ImmutableReferenceCreatedResult.builder()
            .namedRef(namedRef)
            .hash(objIdToHash(reference.pointer()))
            .build();
      }
    } catch (RefAlreadyExistsException e) {
      throw referenceAlreadyExists(namedRef);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    } catch (RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReferenceAssignedResult assign(NamedRef namedRef, Hash expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);
    Reference expected;
    try {
      expected = referenceLogic.getReferenceForUpdate(refName);
    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    }

    try {
      Hash currentCommitId;
      try {
        CommitObj head = commitLogic(persist).headCommit(expected);
        currentCommitId = head != null ? objIdToHash(head.id()) : NO_ANCESTOR;
      } catch (ObjNotFoundException e) {
        // Special case, when the commit object does not exist. This situation can happen in a
        // multi-zone/multi-region database setup, when the "primary write target cluster" fails and
        // the replica cluster(s) had no chance to receive the commit object but the change to the
        // reference. In case the zone/region is permanently switched (think: data center flooded /
        // burned down), this code path allows to assign the branch to another commit object, even
        // if the current HEAD's commit object does not exist.
        currentCommitId = expectedHash;
      }

      verifyExpectedHash(currentCommitId, namedRef, expectedHash);
      expected =
          reference(
              refName,
              hashToObjId(expectedHash),
              false,
              expected.createdAtMicros(),
              expected.extendedInfoObj(),
              expected.previousPointers());

      ObjId newPointer = hashToObjId(targetHash);
      if (!EMPTY_OBJ_ID.equals(newPointer) && persist.fetchObjType(newPointer) != COMMIT) {
        throw RefMapping.hashNotFound(targetHash);
      }

      referenceLogic.assignReference(expected, newPointer);
      return ImmutableReferenceAssignedResult.builder()
          .namedRef(namedRef)
          .previousHash(objIdToHash(expected.pointer()))
          .currentHash(objIdToHash(newPointer))
          .build();

    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    } catch (RefConditionFailedException e) {
      throw referenceConflictException(
          namedRef, objIdToHash(e.reference().pointer()), expected.pointer());
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef namedRef, Hash hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);

    ObjId expected = EMPTY_OBJ_ID;
    try {
      expected = hashToObjId(hash);
      referenceLogic.deleteReference(refName, expected);
      return ImmutableReferenceDeletedResult.builder()
          .namedRef(namedRef)
          .hash(objIdToHash(expected))
          .build();

    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    } catch (RefConditionFailedException e) {
      RefMapping refMapping = new RefMapping(persist);
      CommitObj headCommit = refMapping.resolveRefHeadForUpdate(namedRef);
      throw referenceConflictException(
          namedRef, objIdToHash(expected), headCommit != null ? headCommit.id() : EMPTY_OBJ_ID);
    } catch (RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReferenceHistory getReferenceHistory(String refName, Integer headCommitsToScan)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    Reference reference = refMapping.resolveNamedRef(refName);

    ImmutableReferenceHistory.Builder history = ImmutableReferenceHistory.builder();

    ConsistencyLogic consistencyLogic = consistencyLogic(persist);

    AtomicBoolean referenceHead = new AtomicBoolean();
    AtomicReference<CommitObj> headCommit = new AtomicReference<>();
    ReferenceHistory.ReferenceHistoryElement headCommitStatus =
        consistencyLogic.checkReference(
            reference,
            commitStatus -> {
              CommitObj commit = commitStatus.commit();
              ReferenceHistory.ReferenceHistoryElement elem =
                  referenceHistoryElement(
                      objIdToHash(commitStatus.id()),
                      statusToCommitConsistency(commitStatus),
                      commit != null ? toCommitMeta(commit) : null);
              if (referenceHead.compareAndSet(false, true)) {
                history.current(elem);
                headCommit.set(commit);
              } else {
                history.addPrevious(elem);
              }
              return elem;
            });

    history.commitLogConsistency(
        checkCommitLog(consistencyLogic, headCommit.get(), headCommitsToScan, headCommitStatus));

    return history.build();
  }

  private static CommitConsistency statusToCommitConsistency(
      ConsistencyLogic.CommitStatus commitStatus) {
    if (commitStatus.commit() == null) {
      if (EMPTY_OBJ_ID.equals(commitStatus.id())) {
        return CommitConsistency.COMMIT_CONSISTENT;
      }
      return CommitConsistency.COMMIT_INCONSISTENT;
    }
    if (!commitStatus.indexObjectsAvailable()) {
      return CommitConsistency.COMMIT_INCONSISTENT;
    }
    if (!commitStatus.contentObjectsAvailable()) {
      return CommitConsistency.COMMIT_CONTENT_INCONSISTENT;
    }
    return CommitConsistency.COMMIT_CONSISTENT;
  }

  /**
   * Evaluates the state of the commit log for {@link #getReferenceHistory(String, Integer)}, if
   * requested. This function returns a result that represents the consistency of the commit log
   * including <em>direct</em> secondary parent commits, the consistency of the "worst" commit.
   */
  private CommitConsistency checkCommitLog(
      ConsistencyLogic consistencyLogic,
      CommitObj headCommit,
      Integer headCommitsToScan,
      ReferenceHistory.ReferenceHistoryElement current) {
    if (headCommitsToScan == null || headCommitsToScan <= 0) {
      return CommitConsistency.NOT_CHECKED;
    }
    if (current.commitConsistency() == CommitConsistency.COMMIT_INCONSISTENT) {
      // The HEAD commit is inconsistent - do not check the log (it can't be more consistent)
      return CommitConsistency.COMMIT_INCONSISTENT;
    }
    if (headCommit == null) {
      // "no commit" - empty reference (EMPTY_OBJ_ID)
      return CommitConsistency.COMMIT_CONSISTENT;
    }

    // This function will check at max 1000 commits in the commit log, or until the configured
    // recorded reference-history interval.
    int toScan = Math.min(headCommitsToScan, 1000);
    long nowMicros = persist.config().currentTimeMicros();
    long inconsistencyWindow =
        TimeUnit.SECONDS.toMicros(persist.config().referencePreviousHeadTimeSpanSeconds());

    // Start with the HEAD's direct parent, the HEAD itself is checked elsewhere.
    ObjId commitId = headCommit.directParent();

    // The "initial" result of the commit log cannot be better than the state of the HEAD, so start
    // with the HEAD's state.
    CommitConsistency result = current.commitConsistency();

    ConsistencyLogic.CommitStatusCallback<ReferenceHistory.ReferenceHistoryElement> checkCallback =
        commitStatus -> {
          CommitObj commit = commitStatus.commit();
          return referenceHistoryElement(
              objIdToHash(commitStatus.id()),
              statusToCommitConsistency(commitStatus),
              commit != null ? toCommitMeta(commit) : null);
        };

    while (toScan-- > 0) {
      if (commitId.equals(EMPTY_OBJ_ID)) {
        break;
      }

      CommitObj currentCommit;

      try {
        currentCommit = persist.fetchTypedObj(commitId, COMMIT, CommitObj.class);
      } catch (ObjNotFoundException e) {
        return CommitConsistency.COMMIT_INCONSISTENT;
      }

      if (nowMicros - currentCommit.created() > inconsistencyWindow) {
        break;
      }

      ReferenceHistory.ReferenceHistoryElement elem =
          consistencyLogic.checkCommit(commitId, checkCallback);
      if (elem.commitConsistency().ordinal() > result.ordinal()) {
        // State of the checked commit is worse than the current state.
        result = elem.commitConsistency();
      }
      for (ObjId secondaryParent : currentCommit.secondaryParents()) {
        elem = consistencyLogic.checkCommit(secondaryParent, checkCallback);
        if (elem.commitConsistency().ordinal() > result.ordinal()) {
          // State of the checked commit is worse than the current state.
          result = elem.commitConsistency();
        }
      }

      if (result == CommitConsistency.COMMIT_INCONSISTENT) {
        // This is the worst state, can return immediately.
        return result;
      }

      commitId = currentCommit.directParent();
    }

    return result;
  }

  @Override
  public ReferenceInfo<CommitMeta> getNamedRef(String refName, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    Reference reference = refMapping.resolveNamedRef(refName);
    NamedRef namedRef = referenceToNamedRef(reference);

    CommitObj head = refMapping.resolveNamedRefHead(reference);

    Optional<CommitObj> baseRefHead = headForBaseReference(refMapping, params);

    CommitLogic commitLogic = commitLogic(persist);
    try {
      return buildReferenceInfo(params, baseRefHead, commitLogic, namedRef, head);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  private Optional<CommitObj> headForBaseReference(RefMapping refMapping, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    NamedRef baseReference = params.getBaseReference();
    if (baseReference == null) {
      return Optional.empty();
    }

    Reference ref = refMapping.resolveNamedRef(baseReference);
    try {
      return Optional.ofNullable(commitLogic(persist).headCommit(ref));
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(baseReference);
    }
  }

  @Override
  public PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    ReferenceLogic referenceLogic = referenceLogic(persist);

    PagingToken token = pagingToken != null ? fromString(pagingToken) : null;

    String prefix;
    if (params.getBranchRetrieveOptions().isRetrieve()
        && !params.getTagRetrieveOptions().isRetrieve()) {
      prefix = RefMapping.REFS_HEADS;
    } else if (!params.getBranchRetrieveOptions().isRetrieve()
        && params.getTagRetrieveOptions().isRetrieve()) {
      prefix = RefMapping.REFS_TAGS;
    } else {
      prefix = RefMapping.REFS;
    }

    PagedResult<Reference, String> result =
        referenceLogic.queryReferences(referencesQuery(token, prefix, false));

    Optional<CommitObj> baseRefHead = headForBaseReference(refMapping, params);

    CommitLogic commitLogic = commitLogic(persist);

    return new FilteringPaginationIterator<>(
        result,
        reference -> {
          try {
            NamedRef namedRef = referenceToNamedRef(reference);
            CommitObj head = commitLogic(persist).headCommit(reference);
            return buildReferenceInfo(params, baseRefHead, commitLogic, namedRef, head);
          } catch (ObjNotFoundException e) {
            throw new RuntimeException("Could not resolve reference " + reference, e);
          }
        }) {

      @Override
      protected String computeTokenForCurrent() {
        Reference c = current();
        return c != null ? tokenFor(c.name()) : null;
      }

      @Override
      public String tokenForEntry(ReferenceInfo<CommitMeta> entry) {
        return tokenFor(namedRefToRefName(entry.getNamedRef()));
      }

      private String tokenFor(String refName) {
        return pagingToken(copyFromUtf8(refName)).asString();
      }
    };
  }

  private ReferenceInfo<CommitMeta> buildReferenceInfo(
      GetNamedRefsParams params,
      Optional<CommitObj> baseRefHead,
      CommitLogic commitLogic,
      NamedRef namedRef,
      CommitObj head)
      throws ObjNotFoundException {
    ImmutableReferenceInfo.Builder<CommitMeta> refInfo =
        ReferenceInfo.<CommitMeta>builder().namedRef(namedRef);

    if (head != null) {
      refInfo.hash(objIdToHash(head.id()));

      RetrieveOptions opts = params.getBranchRetrieveOptions();
      if (namedRef instanceof TagName) {
        opts = params.getTagRetrieveOptions();
      }

      if (opts.isRetrieveCommitMetaForHead()) {
        refInfo.addParentHashes(objIdToHash(head.directParent()));
        head.secondaryParents().forEach(p -> refInfo.addParentHashes(objIdToHash(p)));
        refInfo.headCommitMeta(toCommitMeta(head)).commitSeq(head.seq());
      }

      if (!namedRef.equals(params.getBaseReference())
          && (opts.isComputeAheadBehind() || opts.isComputeCommonAncestor())) {
        if (baseRefHead.isPresent()) {
          CommitObj baseHead = baseRefHead.get();
          try {
            ObjId commonAncestorId = commitLogic.findCommonAncestor(baseHead.id(), head.id());
            refInfo.commonAncestor(objIdToHash(commonAncestorId));

            if (opts.isComputeAheadBehind()) {
              CommitObj commonAncestor = commitLogic.fetchCommit(commonAncestorId);
              long commonAncestorSeq = commonAncestor.seq();
              refInfo.aheadBehind(
                  CommitsAheadBehind.of(
                      (int) (head.seq() - commonAncestorSeq),
                      (int) (baseHead.seq() - commonAncestorSeq)));
            }
          } catch (NoSuchElementException e) {
            // no common ancestor
            refInfo.commonAncestor(NO_ANCESTOR);

            if (opts.isComputeAheadBehind()) {
              refInfo.aheadBehind(CommitsAheadBehind.of((int) head.seq(), (int) baseHead.seq()));
            }
          }
        } else {
          refInfo.commonAncestor(NO_ANCESTOR);
          if (opts.isComputeAheadBehind()) {
            refInfo.aheadBehind(CommitsAheadBehind.of((int) head.seq(), 0));
          }
        }
      }

    } else {
      refInfo.hash(NO_ANCESTOR);
    }

    return refInfo.build();
  }

  static <R> R emptyOrNotFound(Ref ref, R namedRefResult) throws ReferenceNotFoundException {
    if (ref instanceof Hash && !NO_ANCESTOR.equals(ref)) {
      throw RefMapping.hashNotFound((Hash) ref);
    }
    return namedRefResult;
  }

  @Override
  public PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return emptyOrNotFound(ref, PaginationIterator.empty());
    }
    CommitLogic commitLogic = commitLogic(persist);
    PagedResult<CommitObj, ObjId> result = commitLogic.commitLog(commitLogQuery(head.id()));

    ContentMapping contentMapping = new ContentMapping(persist);

    return new FilteringPaginationIterator<>(
        result,
        commitObj -> {
          try {
            return contentMapping.commitObjToCommit(fetchAdditionalInfo, commitObj);
          } catch (ObjNotFoundException e) {
            throw new RuntimeException("Could not map commit", e);
          }
        }) {
      @Override
      protected String computeTokenForCurrent() {
        CommitObj c = current();
        return c != null ? pagingToken(c.id().asBytes()).asString() : null;
      }

      @Override
      public String tokenForEntry(Commit entry) {
        return pagingToken(entry.getHash().asBytes()).asString();
      }
    };
  }

  @Override
  public List<IdentifiedContentKey> getIdentifiedKeys(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return emptyList();
    }
    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndex(head, Optional.empty());

    return keys.stream()
        .map(
            key -> {
              StoreKey storeKey = keyToStoreKey(key);
              StoreIndexElement<CommitOp> indexElement = index.get(storeKey);
              if (indexElement == null) {
                return null;
              }
              CommitOp content = indexElement.content();
              UUID contentId = content.contentId();

              return buildIdentifiedKey(
                  key,
                  index,
                  contentTypeForPayload(content.payload()),
                  contentId != null ? contentId.toString() : null,
                  x -> null);
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public PaginationIterator<KeyEntry> getKeys(
      Ref ref, String pagingToken, boolean withContent, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    KeyRanges keyRanges = keyRanges(pagingToken, keyRestrictions);

    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return emptyOrNotFound(ref, PaginationIterator.empty());
    }
    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndex(head, Optional.empty());

    Iterator<StoreIndexElement<CommitOp>> result =
        index.iterator(keyRanges.beginStoreKey(), keyRanges.endStoreKey(), false);
    ContentMapping contentMapping = new ContentMapping(persist);

    BiPredicate<ContentKey, Content.Type> contentKeyPredicate =
        keyRestrictions.contentKeyPredicate();

    Predicate<StoreIndexElement<CommitOp>> stopPredicate;
    ContentKey prefixKey = keyRestrictions.prefixKey();
    if (prefixKey != null) {
      StoreKey prefix = keyToStoreKeyNoVariant(prefixKey);
      stopPredicate = indexElement -> !indexElement.key().startsWithElementsOrParts(prefix);
    } else {
      stopPredicate = x -> false;
    }

    // "Base" iterator, which maps StoreIndexElement objects to ContentKey and CommitOp and also
    // filters out non-content keys and non-live CommitOps.
    Iterator<ContentKeyWithCommitOp> keyAndOp =
        new AbstractIterator<>() {
          @CheckForNull
          @Override
          protected ContentKeyWithCommitOp computeNext() {
            while (true) {
              if (!result.hasNext()) {
                return endOfData();
              }

              StoreIndexElement<CommitOp> indexElement = result.next();

              if (stopPredicate.test(indexElement)) {
                return endOfData();
              }

              StoreKey storeKey = indexElement.key();

              if (!indexElement.content().action().exists()
                  || !indexElement.key().endsWithElement(CONTENT_DISCRIMINATOR)) {
                continue;
              }

              ContentKey key = storeKeyToKey(storeKey);

              if (contentKeyPredicate != null
                  && !contentKeyPredicate.test(
                      key, contentTypeForPayload(indexElement.content().payload()))) {
                continue;
              }

              return new ContentKeyWithCommitOp(storeKey, key, indexElement.content());
            }
          }
        };

    // "Fetch content" iterator - same as the "base" iterator when not fetching the content,
    // fetches contents in batches if 'withContent == true'.
    Iterator<ContentKeyWithCommitOp> fetchContent =
        withContent
            ? new AbstractIterator<>() {
              final List<ContentKeyWithCommitOp> batch =
                  new ArrayList<>(GET_KEYS_CONTENT_BATCH_SIZE);

              Iterator<ContentKeyWithCommitOp> current;

              @CheckForNull
              @Override
              protected ContentKeyWithCommitOp computeNext() {
                Iterator<ContentKeyWithCommitOp> c = current;
                if (c != null && c.hasNext()) {
                  return c.next();
                }

                for (int i = 0; i < GET_KEYS_CONTENT_BATCH_SIZE; i++) {
                  if (!keyAndOp.hasNext()) {
                    break;
                  }
                  batch.add(keyAndOp.next());
                }

                if (batch.isEmpty()) {
                  current = null;
                  return endOfData();
                }

                try {
                  Map<ContentKey, Content> contents =
                      contentMapping.fetchContents(
                          index, batch.stream().map(op -> op.key).collect(Collectors.toList()));
                  for (ContentKeyWithCommitOp op : batch) {
                    op.content = contents.get(op.key);
                  }
                } catch (ObjNotFoundException e) {
                  throw new RuntimeException("Could not fetch or map content", e);
                }
                current = new ArrayList<>(batch).iterator();
                batch.clear();
                return current.next();
              }
            }
            : keyAndOp;

    // "Final" iterator, adding functionality for paging. Needs to be a separate instance, because
    // we cannot use the "base" iterator to provide the token for the "current" entry.
    return new PaginationIterator<>() {
      ContentKeyWithCommitOp current;

      @Override
      public String tokenForCurrent() {
        return token(current.storeKey);
      }

      @Override
      public String tokenForEntry(KeyEntry entry) {
        return token(keyToStoreKey(entry.getKey().contentKey()));
      }

      private String token(StoreKey storeKey) {
        return pagingToken(copyFromUtf8(storeKey.rawString())).asString();
      }

      @Override
      public boolean hasNext() {
        return fetchContent.hasNext();
      }

      @Override
      public KeyEntry next() {
        ContentKeyWithCommitOp c = current = fetchContent.next();

        if (c.content != null) {
          return KeyEntry.of(buildIdentifiedKey(c.key, index, c.content, x -> null), c.content);
        }

        try {
          UUID contentId = c.commitOp.contentId();
          String contentIdString;
          if (contentId == null) {
            // this should only be hit by imported legacy nessie repos
            if (c.content == null) {
              c.content =
                  contentMapping.fetchContent(
                      requireNonNull(c.commitOp.value(), "Required value pointer is null"));
            }
            contentIdString = c.content.getId();
          } else {
            contentIdString = contentId.toString();
          }
          Content.Type contentType = contentTypeForPayload(c.commitOp.payload());
          return KeyEntry.of(
              buildIdentifiedKey(c.key, index, contentType, contentIdString, x -> null));
        } catch (ObjNotFoundException e) {
          throw new RuntimeException("Could not fetch or map content", e);
        }
      }

      @Override
      public void close() {}
    };
  }

  static final class ContentKeyWithCommitOp {
    final StoreKey storeKey;
    final ContentKey key;
    final CommitOp commitOp;
    Content content;

    ContentKeyWithCommitOp(StoreKey storeKey, ContentKey key, CommitOp commitOp) {
      this.storeKey = storeKey;
      this.key = key;
      this.commitOp = commitOp;
    }
  }

  @Override
  public ContentResult getValue(Ref ref, ContentKey key, boolean returnNotFound)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return getValueNotFound(key, returnNotFound, emptyImmutableIndex(COMMIT_OP_SERIALIZER));
    }
    try {

      StoreKey storeKey = keyToStoreKey(key);
      IndexesLogic indexesLogic = indexesLogic(persist);
      StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndex(head, Optional.empty());

      index.loadIfNecessary(singleton(storeKey));

      StoreIndexElement<CommitOp> indexElement = index.get(storeKey);
      if (indexElement == null || !indexElement.content().action().exists()) {
        return getValueNotFound(key, returnNotFound, index);
      }

      ContentMapping contentMapping = new ContentMapping(persist);
      Content content =
          contentMapping.fetchContent(
              requireNonNull(indexElement.content().value(), "Required value pointer is null"));

      IdentifiedContentKey identifiedKey = buildIdentifiedKey(key, index, content, x -> null);

      return contentResult(identifiedKey, content, null);
    } catch (ObjNotFoundException e) {
      throw objectNotFound(e);
    }
  }

  private static ContentResult getValueNotFound(
      ContentKey key, boolean returnNotFound, StoreIndex<CommitOp> index) {
    if (returnNotFound) {
      IdentifiedContentKey identifiedKey = buildIdentifiedKey(key, index, null, null, x -> null);
      return contentResult(identifiedKey, null, null);
    }
    return null;
  }

  static IdentifiedContentKey buildIdentifiedKey(
      ContentKey key,
      StoreIndex<CommitOp> index,
      Content content,
      Function<List<String>, UUID> newContentIds) {
    return buildIdentifiedKey(key, index, content.getType(), content.getId(), newContentIds);
  }

  static IdentifiedContentKey buildIdentifiedKey(
      ContentKey key,
      StoreIndex<CommitOp> index,
      int payload,
      UUID contentId,
      Function<List<String>, UUID> newContentIds) {
    String cid = contentId != null ? contentId.toString() : null;
    Content.Type contentType = contentTypeForPayload(payload);
    return buildIdentifiedKey(key, index, contentType, cid, newContentIds);
  }

  static IdentifiedContentKey buildIdentifiedKey(
      ContentKey key,
      StoreIndex<CommitOp> index,
      Content.Type contentType,
      String contentId,
      Function<List<String>, UUID> newContentIds) {
    return identifiedContentKeyFromContent(
        key,
        contentType,
        contentId,
        path -> {
          StoreIndexElement<CommitOp> pathIndexElement = index.get(keyToStoreKey(path));
          UUID id = null;
          if (pathIndexElement != null && pathIndexElement.content().action().exists()) {
            id = pathIndexElement.content().contentId();
          }
          if (id == null) {
            id = newContentIds.apply(path);
          }
          return id != null ? id.toString() : null;
        });
  }

  @Override
  public Map<ContentKey, ContentResult> getValues(
      Ref ref, Collection<ContentKey> keys, boolean returnNotFound)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);

    try {
      IndexesLogic indexesLogic = indexesLogic(persist);
      StoreIndex<CommitOp> index =
          head != null
              ? indexesLogic.buildCompleteIndex(head, Optional.empty())
              : emptyImmutableIndex(COMMIT_OP_SERIALIZER);

      ContentMapping contentMapping = new ContentMapping(persist);
      Map<ContentKey, Content> fetched = contentMapping.fetchContents(index, keys);
      Map<ContentKey, ContentResult> result = newHashMapWithExpectedSize(keys.size());

      for (ContentKey key : keys) {
        Content content = fetched.get(key);
        if (content != null) {
          result.put(
              key,
              contentResult(buildIdentifiedKey(key, index, content, x -> null), content, null));
        } else if (returnNotFound) {
          IdentifiedContentKey identifiedKey =
              buildIdentifiedKey(key, index, null, null, x -> null);
          result.put(key, contentResult(identifiedKey, null, null));
        }
      }

      return result;
    } catch (ObjNotFoundException e) {
      throw objectNotFound(e);
    }
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
    return committingOperation(
        "commit",
        branch,
        referenceHash,
        persist,
        CommitImpl::new,
        (commitImpl, retryState) ->
            commitImpl.commit(retryState, metadata, operations, validator, addedContents));
  }

  @Override
  public MergeResult merge(MergeOp mergeOp)
      throws ReferenceNotFoundException, ReferenceConflictException {
    CommitterSupplier<Merge> supplier = MergeSquashImpl::new;

    if (mergeOp.dryRun()) {
      supplier = dryRunCommitterSupplier(supplier);
    }

    MergeResult mergeResult =
        committingOperation(
            "merge",
            mergeOp.toBranch(),
            mergeOp.expectedHash(),
            persist,
            supplier,
            (merge, retryState) -> merge.merge(retryState, mergeOp));

    return mergeTransplantResponse(mergeResult);
  }

  @Override
  public TransplantResult transplant(TransplantOp transplantOp)
      throws ReferenceNotFoundException, ReferenceConflictException {

    CommitterSupplier<Transplant> supplier = TransplantIndividualImpl::new;

    if (transplantOp.dryRun()) {
      supplier = dryRunCommitterSupplier(supplier);
    }

    TransplantResult mergeResult =
        committingOperation(
            "transplant",
            transplantOp.toBranch(),
            transplantOp.expectedHash(),
            persist,
            supplier,
            (transplant, retryState) -> transplant.transplant(retryState, transplantOp));

    return mergeTransplantResponse(mergeResult);
  }

  private <R extends MergeTransplantResultBase> R mergeTransplantResponse(R mergeResult)
      throws MergeConflictException {
    if (!mergeResult.wasSuccessful()) {
      throw new MergeConflictException(
          String.format(
              "The following keys have been changed in conflict: %s",
              mergeResult.getDetails().entrySet().stream()
                  .filter(e -> e.getValue().getConflict() != null)
                  .map(Map.Entry::getKey)
                  .sorted()
                  .map(key -> String.format("'%s'", key))
                  .collect(Collectors.joining(", "))),
          mergeResult);
    }

    return mergeResult;
  }

  @Override
  public PaginationIterator<Diff> getDiffs(
      Ref from, Ref to, String pagingToken, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException {
    KeyRanges keyRanges = keyRanges(pagingToken, keyRestrictions);

    RefMapping refMapping = new RefMapping(persist);

    CommitObj fromCommit = refMapping.resolveRefHead(from);
    if (fromCommit == null) {
      emptyOrNotFound(from, null);
    }

    CommitObj toCommit = refMapping.resolveRefHead(to);
    if (toCommit == null) {
      emptyOrNotFound(to, null);
    }

    CommitLogic commitLogic = commitLogic(persist);
    DiffPagedResult<DiffEntry, StoreKey> diffIter =
        commitLogic.diff(
            diffQuery(
                keyRanges.pagingTokenObj(),
                fromCommit,
                toCommit,
                keyRanges.beginStoreKey(),
                keyRanges.endStoreKey(),
                true,
                null));

    ContentMapping contentMapping = new ContentMapping(persist);

    BiPredicate<ContentKey, Content.Type> contentKeyPredicate =
        keyRestrictions.contentKeyPredicate();
    Predicate<DiffEntry> keyPred =
        contentKeyPredicate != null
            ? d -> {
              ContentKey key = storeKeyToKey(d.key());
              return key != null
                  && (contentKeyPredicate.test(key, contentTypeForPayload(d.fromPayload()))
                      || contentKeyPredicate.test(key, contentTypeForPayload(d.toPayload())));
            }
            : x -> true;

    Predicate<DiffEntry> stopPredicate;
    ContentKey prefixKey = keyRestrictions.prefixKey();
    if (prefixKey != null) {
      StoreKey prefix = keyToStoreKeyNoVariant(prefixKey);
      stopPredicate = d -> !d.key().startsWithElementsOrParts(prefix);
    } else {
      stopPredicate = x -> false;
    }

    return new FilteringPaginationIterator<>(
        diffIter,
        d -> {
          Function<ObjId, Content> contentFetcher =
              id -> {
                try {
                  return contentMapping.fetchContent(id);
                } catch (ObjNotFoundException e) {
                  throw new RuntimeException(e.getMessage());
                }
              };
          ContentKey contentKey = storeKeyToKey(d.key());

          IdentifiedContentKey fromKey =
              d.fromId() != null
                  ? buildIdentifiedKey(
                      contentKey,
                      diffIter.fromIndex(),
                      contentTypeForPayload(d.fromPayload()),
                      d.fromContentId() != null
                          ? requireNonNull(d.fromContentId()).toString()
                          : null,
                      x -> null)
                  : null;

          IdentifiedContentKey toKey =
              d.toId() != null
                  ? (Objects.equals(d.fromContentId(), d.toContentId())
                      ? fromKey
                      : buildIdentifiedKey(
                          contentKey,
                          diffIter.toIndex(),
                          contentTypeForPayload(d.toPayload()),
                          d.toContentId() != null
                              ? requireNonNull(d.toContentId()).toString()
                              : null,
                          x -> null))
                  : null;

          return Diff.of(
              fromKey,
              toKey,
              Optional.ofNullable(d.fromId()).map(contentFetcher),
              Optional.ofNullable(d.toId()).map(contentFetcher));
        },
        keyPred,
        stopPredicate) {
      @Override
      protected String computeTokenForCurrent() {
        DiffEntry c = current();
        return c != null ? tokenFor(c.key()) : null;
      }

      @Override
      public String tokenForEntry(Diff entry) {
        return tokenFor(keyToStoreKeyMin(entry.contentKey()));
      }

      private String tokenFor(StoreKey storeKey) {
        return pagingToken(copyFromUtf8(storeKey.rawString())).asString();
      }
    };
  }

  @Override
  public List<RepositoryConfig> getRepositoryConfig(
      Set<RepositoryConfig.Type> repositoryConfigTypes) {
    return new RepositoryConfigBackend(persist).getConfigs(repositoryConfigTypes);
  }

  @Override
  public RepositoryConfig updateRepositoryConfig(RepositoryConfig repositoryConfig)
      throws ReferenceConflictException {
    return new RepositoryConfigBackend(persist).updateConfig(repositoryConfig);
  }
}
