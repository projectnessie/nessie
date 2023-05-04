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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.DiffQuery.diffQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.fromString;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.versionstore.BaseCommitHelper.committingOperation;
import static org.projectnessie.versioned.storage.versionstore.BaseCommitHelper.dryRunCommitterSupplier;
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
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;
import static org.projectnessie.versioned.store.DefaultStoreWorker.contentTypeForPayload;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableReferenceInfo;
import org.projectnessie.versioned.ImmutableRepositoryInformation;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.RefLogDetails;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceInfo.CommitsAheadBehind;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RepositoryInformation;
import org.projectnessie.versioned.TagName;
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
import org.projectnessie.versioned.storage.common.logic.DiffEntry;
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

  private final Persist persist;

  @SuppressWarnings("unused")
  public VersionStoreImpl() {
    this(null);
  }

  public VersionStoreImpl(Persist persist) {
    this.persist = persist;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
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
  @jakarta.annotation.Nonnull
  @Override
  public Hash noAncestorHash() {
    return RefMapping.NO_ANCESTOR;
  }

  @Override
  public Hash hashOnReference(NamedRef namedRef, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveNamedRefHead(namedRef);

    CommitObj commit = refMapping.commitInChain(namedRef, head, hashOnReference);
    return commit != null ? objIdToHash(commit.id()) : NO_ANCESTOR;
  }

  @Override
  public Hash create(NamedRef namedRef, Optional<Hash> targetHash)
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
        referenceLogic.getReference(mustNotExist);
        // A tag with the same name as the branch being created (or a branch with the same name
        // as the tag being created) already exists.
        throw referenceAlreadyExists(namedRef);
      } catch (RefNotFoundException good) {
        return objIdToHash(
            referenceLogic.createReference(namedRefToRefName(namedRef), objId).pointer());
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
  public void assign(NamedRef namedRef, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);
    Reference expected;
    try {
      expected = referenceLogic.getReference(refName);
    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    }

    try {
      if (expectedHash.isPresent()) {
        CommitObj head = commitLogic(persist).headCommit(expected);
        Hash currentCommitId = head != null ? objIdToHash(head.id()) : NO_ANCESTOR;
        verifyExpectedHash(currentCommitId, namedRef, expectedHash);
        expected = reference(refName, hashToObjId(expectedHash.get()), false);
      }

      ObjId newPointer = hashToObjId(targetHash);
      if (!EMPTY_OBJ_ID.equals(newPointer) && persist.fetchObjType(newPointer) != COMMIT) {
        throw RefMapping.hashNotFound(targetHash);
      }

      referenceLogic.assignReference(expected, newPointer);
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
  public Hash delete(NamedRef namedRef, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);

    ObjId expected = EMPTY_OBJ_ID;
    try {
      expected =
          hash.isPresent()
              ? hashToObjId(hash.get())
              : referenceLogic.getReference(refName).pointer();
      referenceLogic.deleteReference(refName, expected);
      return objIdToHash(expected);
    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    } catch (RefConditionFailedException e) {
      RefMapping refMapping = new RefMapping(persist);
      CommitObj headCommit = refMapping.resolveRefHead(namedRef);
      throw referenceConflictException(
          namedRef, objIdToHash(expected), headCommit != null ? headCommit.id() : EMPTY_OBJ_ID);
    } catch (RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
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

    PagedResult<Reference, String> result =
        referenceLogic.queryReferences(referencesQuery(token, RefMapping.REFS, false));

    Optional<CommitObj> baseRefHead = headForBaseReference(refMapping, params);

    CommitLogic commitLogic = commitLogic(persist);

    return new FilteringPaginationIterator<Reference, ReferenceInfo<CommitMeta>>(
        result,
        reference -> {
          try {
            NamedRef namedRef = referenceToNamedRef(reference);
            CommitObj head = commitLogic(persist).headCommit(reference);
            return buildReferenceInfo(params, baseRefHead, commitLogic, namedRef, head);
          } catch (ReferenceNotFoundException | ObjNotFoundException e) {
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
      throws ObjNotFoundException, ReferenceNotFoundException {
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
              CommitObj commonAncestor =
                  persist.fetchTypedObj(commonAncestorId, COMMIT, CommitObj.class);
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

    return new FilteringPaginationIterator<CommitObj, Commit>(
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
  public PaginationIterator<KeyEntry> getKeys(Ref ref, String pagingToken, boolean withContent)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return emptyOrNotFound(ref, PaginationIterator.empty());
    }
    IndexesLogic indexesLogic = indexesLogic(persist);
    StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndex(head, Optional.empty());

    StoreKey begin =
        pagingToken != null && !pagingToken.isEmpty()
            ? keyFromString(fromString(pagingToken).token().toStringUtf8())
            : null;
    StoreKey end = null;

    Iterator<StoreIndexElement<CommitOp>> result = index.iterator(begin, end, false);
    ContentMapping contentMapping = new ContentMapping(persist);

    return new FilteringPaginationIterator<StoreIndexElement<CommitOp>, KeyEntry>(
        result,
        indexElement -> {
          try {
            ContentKey key = storeKeyToKey(indexElement.key());
            CommitOp commitOp = indexElement.content();
            Content.Type contentType = contentTypeForPayload((byte) commitOp.payload());

            if (withContent) {
              Content c =
                  contentMapping.fetchContent(
                      requireNonNull(
                          indexElement.content().value(), "Required value pointer is null"));
              return KeyEntry.of(contentType, key, c);
            }

            UUID contentId = commitOp.contentId();
            String contentIdString =
                contentId != null ? contentId.toString() : contentIdFromContent(commitOp);
            return KeyEntry.of(contentType, key, contentIdString);
          } catch (ObjNotFoundException e) {
            throw new RuntimeException("Could not fetch or map content", e);
          }
        },
        indexElement ->
            indexElement.content().action().exists()
                // Note: key==null, if not the "main universe" or not a "content" discriminator
                && storeKeyToKey(indexElement.key()) != null) {
      @Override
      protected String computeTokenForCurrent() {
        StoreIndexElement<CommitOp> c = current();
        return c != null ? token(c.key()) : null;
      }

      @Override
      public String tokenForEntry(KeyEntry entry) {
        return token(keyToStoreKey(entry.getKey()));
      }

      private String token(StoreKey storeKey) {
        return pagingToken(copyFromUtf8(storeKey.rawString())).asString();
      }
    };
  }

  private String contentIdFromContent(CommitOp commitOp) throws ObjNotFoundException {
    return new ContentMapping(persist)
        .fetchContent(requireNonNull(commitOp.value(), "Required value pointer is null"))
        .getId();
  }

  @Override
  public Content getValue(Ref ref, ContentKey key) throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return emptyOrNotFound(ref, null);
    }
    try {

      StoreKey storeKey = keyToStoreKey(key);
      IndexesLogic indexesLogic = indexesLogic(persist);
      StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndex(head, Optional.empty());

      index.loadIfNecessary(singleton(storeKey));

      StoreIndexElement<CommitOp> indexElement = index.get(storeKey);
      if (indexElement == null || !indexElement.content().action().exists()) {
        return null;
      }

      ContentMapping contentMapping = new ContentMapping(persist);
      return contentMapping.fetchContent(
          requireNonNull(indexElement.content().value(), "Required value pointer is null"));
    } catch (ObjNotFoundException e) {
      throw objectNotFound(e);
    }
  }

  @Override
  public Map<ContentKey, Content> getValues(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);
    CommitObj head = refMapping.resolveRefHead(ref);
    if (head == null) {
      return emptyOrNotFound(ref, emptyMap());
    }

    try {
      IndexesLogic indexesLogic = indexesLogic(persist);
      StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndex(head, Optional.empty());

      // Eagerly bulk-(pre)fetch the requested keys
      index.loadIfNecessary(
          keys.stream().map(TypeMapping::keyToStoreKey).collect(Collectors.toSet()));

      Map<ObjId, ContentKey> idsToKeys = new HashMap<>();
      for (ContentKey key : keys) {
        StoreKey storeKey = keyToStoreKey(key);
        StoreIndexElement<CommitOp> indexElement = index.get(storeKey);
        if (indexElement == null || !indexElement.content().action().exists()) {
          continue;
        }

        idsToKeys.put(
            requireNonNull(indexElement.content().value(), "Required value pointer is null"), key);
      }

      ContentMapping contentMapping = new ContentMapping(persist);
      return contentMapping.fetchContents(idsToKeys);
    } catch (ObjNotFoundException e) {
      throw objectNotFound(e);
    }
  }

  @Override
  public Hash commit(
      @Nonnull @jakarta.annotation.Nonnull BranchName branch,
      @Nonnull @jakarta.annotation.Nonnull Optional<Hash> referenceHash,
      @Nonnull @jakarta.annotation.Nonnull CommitMeta metadata,
      @Nonnull @jakarta.annotation.Nonnull List<Operation> operations,
      @Nonnull @jakarta.annotation.Nonnull Callable<Void> validator,
      @Nonnull @jakarta.annotation.Nonnull BiConsumer<ContentKey, String> addedContents)
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
  public MergeResult<Commit> merge(
      Hash fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {

    CommitterSupplier<Merge> supplier =
        keepIndividualCommits ? MergeIndividualImpl::new : MergeSquashImpl::new;

    if (dryRun) {
      supplier = dryRunCommitterSupplier(supplier);
    }

    MergeBehaviors mergeBehaviors =
        new MergeBehaviors(keepIndividualCommits, mergeKeyBehaviors, defaultMergeBehavior);

    MergeResult<Commit> mergeResult =
        committingOperation(
            "merge",
            toBranch,
            expectedHash,
            persist,
            supplier,
            (merge, retryState) ->
                merge.merge(retryState, fromHash, updateCommitMetadata, mergeBehaviors, dryRun));

    return mergeTransplantResponse(mergeResult);
  }

  @Override
  public MergeResult<Commit> transplant(
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<CommitMeta> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {

    CommitterSupplier<Transplant> supplier =
        keepIndividualCommits ? TransplantIndividualImpl::new : TransplantSquashImpl::new;

    if (dryRun) {
      supplier = dryRunCommitterSupplier(supplier);
    }

    MergeBehaviors mergeBehaviors =
        new MergeBehaviors(keepIndividualCommits, mergeKeyBehaviors, defaultMergeBehavior);

    MergeResult<Commit> mergeResult =
        committingOperation(
            "transplant",
            targetBranch,
            referenceHash,
            persist,
            supplier,
            (transplant, retryState) ->
                transplant.transplant(
                    retryState,
                    sequenceToTransplant,
                    updateCommitMetadata,
                    mergeBehaviors,
                    dryRun));

    return mergeTransplantResponse(mergeResult);
  }

  private MergeResult<Commit> mergeTransplantResponse(MergeResult<Commit> mergeResult)
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
  public PaginationIterator<Diff> getDiffs(Ref from, Ref to, String pagingToken)
      throws ReferenceNotFoundException {
    RefMapping refMapping = new RefMapping(persist);

    CommitObj fromCommit = refMapping.resolveRefHead(from);
    if (fromCommit == null) {
      emptyOrNotFound(from, null);
    }

    CommitObj toCommit = refMapping.resolveRefHead(to);
    if (toCommit == null) {
      emptyOrNotFound(to, null);
    }

    PagingToken token = pagingToken != null ? fromString(pagingToken) : null;
    StoreKey start = token != null ? keyFromString(token.token().toStringUtf8()) : null;
    StoreKey end = null;

    CommitLogic commitLogic = commitLogic(persist);
    PagedResult<DiffEntry, StoreKey> diffIter =
        commitLogic.diff(diffQuery(token, fromCommit, toCommit, start, end, true));

    ContentMapping contentMapping = new ContentMapping(persist);

    return new FilteringPaginationIterator<DiffEntry, Diff>(
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
          return Diff.of(
              storeKeyToKey(d.key()),
              Optional.ofNullable(d.fromId()).map(contentFetcher),
              Optional.ofNullable(d.toId()).map(contentFetcher));
        },
        d -> storeKeyToKey(d.key()) != null) {
      @Override
      protected String computeTokenForCurrent() {
        DiffEntry c = current();
        return c != null ? tokenFor(c.key()) : null;
      }

      @Override
      public String tokenForEntry(Diff entry) {
        return tokenFor(keyToStoreKey(entry.getKey()));
      }

      private String tokenFor(StoreKey storeKey) {
        return pagingToken(copyFromUtf8(storeKey.rawString())).asString();
      }
    };
  }

  @Override
  @Deprecated
  public Stream<RefLogDetails> getRefLog(Hash refLogId) {
    throw new UnsupportedOperationException();
  }
}
