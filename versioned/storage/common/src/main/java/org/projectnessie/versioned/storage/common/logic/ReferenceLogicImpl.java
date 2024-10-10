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
package org.projectnessie.versioned.storage.common.logic;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CommitConflict.ConflictType.KEY_EXISTS;
import static org.projectnessie.versioned.storage.common.logic.CommitRetry.commitRetry;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REFS;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.emptyPagingToken;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;
import static org.projectnessie.versioned.storage.common.logic.ReferenceLogicImpl.CommitReferenceResult.Kind.ADDED_TO_INDEX;
import static org.projectnessie.versioned.storage.common.logic.ReferenceLogicImpl.CommitReferenceResult.Kind.REF_ROW_EXISTS;
import static org.projectnessie.versioned.storage.common.logic.ReferenceLogicImpl.CommitReferenceResult.Kind.REF_ROW_MISSING;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.REF;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.Reference.INTERNAL_PREFIX;
import static org.projectnessie.versioned.storage.common.persist.Reference.isInternalReferenceName;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.CommitWrappedException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Named references management for non-transactional databases.
 *
 * <p>Provides the actual logic to access and maintain references, focused on non-transactional
 * databases. Transactional databases might get a simplified implementation that relies on the
 * database transactions.
 *
 * <p>Each reference is maintained in one database row. Updates are performed atomically, using CAS
 * operations when necessary.
 *
 * <p>A reference is considered as "created" and "not deleted", if it can be found via {@link
 * Persist#fetchReference(String)} and its {@link Reference#deleted() deleted flag} is {@code
 * false}. All other states, including the not-found state, are subject to recovery as described
 * below.
 *
 * <h3>Reference name index</h3>
 *
 * <p>Note that the {@link Persist low-level storage layer} does not provide a functionality to
 * maintain reference name indexes.
 *
 * <p>This logical layer on top of the storage layer maintains the set of reference names via the
 * internal, well known reference (via {@link InternalRef#REF_REFS}). Reference creations and
 * deletions are tracked in that internal reference using the exising mechanisms using commits and
 * {@link StoreIndex key-indexes}.
 *
 * <p>The logical approaches to create and delete a reference are designed to allow even
 * non-transactional databases to consistently delete a reference, where non-transactional databases
 * additionally run logic to re-run a previously failed process to create or delete a reference.
 *
 * <h3>Create reference / Logical approach</h3>
 *
 * <p>The <em>logical</em> approach to create a reference is as follows:
 *
 * <ol>
 *   <li>Create a commit in {@link InternalRef#REF_REFS} with a {@link Action#ADD} for the reference
 *       using a {@link RefObj} payload, which contains the reference information used during
 *       creation. Implementations must only commit, if the reference is not present, to allow the
 *       resume/recovery process described below.
 *   <li>After the reference has been added to {@link InternalRef#REF_REFS}, call {@link
 *       Persist#addReference(Reference)}, which creates the atomically updatable entry.
 * </ol>
 *
 * <p>Note: Updates to a named reference are <em>not</em> tracked in {@link InternalRef#REF_REFS}.
 *
 * <h3>Delete reference / Logical approach</h3>
 *
 * <ol>
 *   <li>The {@link Reference} is marked as {@link Reference#deleted()} using {@link
 *       Persist#markReferenceAsDeleted(Reference)}.
 *   <li>Then the reference deletion is tracked by creating a commit in {@link InternalRef#REF_REFS}
 *       using a {@link Action#REMOVE} operation. Implementations must only commit, if the reference
 *       is present, to allow the resume/recovery process described below.
 *   <li>Finally, the {@link Reference} is purged (row physically deleted) via {@link
 *       Persist#purgeReference(Reference)}.
 * </ol>
 *
 * <h3>Fetching references</h3>
 *
 * Accessing references by their full name is performed via {@link Persist#fetchReference(String)}
 * (or {@link Persist#fetchReferences(String[])}) and follows the resume/recovery process described
 * below.
 *
 * <h3>Listing references</h3>
 *
 * Listing/querying references is performed via the tip of {@link InternalRef#REF_REFS} and then
 * {@link Persist#fetchReferences(String[])} chunks of references to inquire their tips/HEADs.
 *
 * <h3>Non-transactional resume/recovery</h3>
 *
 * Transactional databases can safely use the native consistent update mechanisms provided by the
 * database. But non-transactional databases need some logic, which works as follows whenever a
 * reference is accessed:
 *
 * <ul>
 *   <li>{@link Persist#fetchReference(String)} (or {@link Persist#fetchReferences(String[])}) is
 *       used to fetch a reference by name.
 *   <li>If the reference is found and {@link Reference#deleted()} is {@code false}, the reference
 *       has been found and can be returned.
 *   <li>If the the {@link Reference#deleted()} flag is {@code true}, it is possible that a previous
 *       delete-reference operation failed in the middle. The implementation must resume the
 *       reference-deletion process described above.
 *   <li>If the reference has not been found via {@link Persist#fetchReference(String)} (or {@link
 *       Persist#fetchReferences(String[])}), check whether the reference name exists in {@link
 *       InternalRef#REF_REFS} and resume the create-reference operation described above.
 * </ul>
 *
 * <h3>Internal references</h3>
 *
 * {@link InternalRef Internal references} are neither visible nor can those be updated via this
 * {@link ReferenceLogicImpl}.
 */
final class ReferenceLogicImpl implements ReferenceLogic {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReferenceLogicImpl.class);
  private static final String REF_REFS_ADVANCED = "ref-refs advanced";

  private final Persist persist;

  ReferenceLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  @Nonnull
  public List<Reference> getReferences(@Nonnull List<String> references) {
    return getReferences(references, persist::fetchReferences);
  }

  @Override
  @Nonnull
  public List<Reference> getReferencesForUpdate(@Nonnull List<String> references) {
    return getReferences(references, persist::fetchReferencesForUpdate);
  }

  private List<Reference> getReferences(
      @Nonnull List<String> references, Function<String[], Reference[]> fetchRefs) {
    int refCount = references.size();
    String[] refsArray;
    int refRefsIndex = references.indexOf(REF_REFS.name());
    if (refRefsIndex != -1) {
      refsArray = references.toArray(new String[refCount]);
    } else {
      refsArray = references.toArray(new String[refCount + 1]);
      refRefsIndex = references.size();
      refsArray[refRefsIndex] = REF_REFS.name();
    }
    Reference[] refs = fetchRefs.apply(refsArray);

    Supplier<SuppliedCommitIndex> refsIndexSupplier = createRefsIndexSupplier(refs[refRefsIndex]);

    List<Reference> r = new ArrayList<>(refCount);
    for (int i = 0; i < refCount; i++) {
      Reference ref = refs[i];

      ref = maybeRecover(references.get(i), ref, refsIndexSupplier);

      if (ref != null && ref.name().startsWith(INTERNAL_PREFIX)) {
        // do not expose internal references
        ref = null;
      }
      r.add(ref);
    }
    return r;
  }

  @Override
  @Nonnull
  public PagedResult<Reference, String> queryReferences(@Nonnull ReferencesQuery referencesQuery) {
    Optional<PagingToken> pagingToken = referencesQuery.pagingToken();

    StoreKey prefix = referencesQuery.referencePrefix().map(StoreKey::keyFromString).orElse(null);

    StoreKey begin =
        pagingToken
            .map(PagingToken::token)
            .map(ByteString::toStringUtf8)
            .map(StoreKey::key)
            .orElse(prefix);

    SuppliedCommitIndex index = createRefsIndexSupplier().get();

    return new QueryIter(index, prefix, begin, referencesQuery.prefetch());
  }

  private final class QueryIter extends AbstractIterator<Reference>
      implements PagedResult<Reference, String> {
    private final SuppliedCommitIndex index;
    private final Iterator<StoreIndexElement<CommitOp>> base;
    private final StoreKey prefix;

    private static final int REFERENCES_BATCH_SIZE = 50;
    private final List<String> referencesBatch;
    private Iterator<Reference> referenceIterator = emptyIterator();

    private QueryIter(
        SuppliedCommitIndex index, StoreKey prefix, StoreKey begin, boolean prefetch) {
      this.index = index;
      this.prefix = prefix;
      this.base = index.index().iterator(begin, null, prefetch);
      this.referencesBatch = new ArrayList<>(REFERENCES_BATCH_SIZE);
    }

    @Override
    protected Reference computeNext() {
      while (true) {
        if (referenceIterator.hasNext()) {
          return referenceIterator.next();
        }

        if (!base.hasNext()) {
          if (fetchReferencesBatch()) {
            continue;
          }
          return endOfData();
        }

        StoreIndexElement<CommitOp> el = base.next();
        StoreKey k = el.key();
        if (prefix == null || k.startsWith(prefix)) {
          String name = k.rawString();
          referencesBatch.add(name);
          if (referencesBatch.size() == REFERENCES_BATCH_SIZE) {
            fetchReferencesBatch();
          }
        }
      }
    }

    private boolean fetchReferencesBatch() {
      if (referencesBatch.isEmpty()) {
        return false;
      }
      Reference[] refs = persist.fetchReferences(referencesBatch.toArray(new String[0]));
      List<Reference> refsList = new ArrayList<>(refs.length);
      for (int i = 0; i < refs.length; i++) {
        Reference ref = refs[i];
        ref = maybeRecover(referencesBatch.get(i), ref, () -> index);
        if (ref != null) {
          refsList.add(ref);
        }
      }

      referencesBatch.clear();
      referenceIterator = refsList.iterator();

      return true;
    }

    @Nonnull
    @Override
    public PagingToken tokenForKey(String key) {
      return key != null ? pagingToken(copyFromUtf8(key)) : emptyPagingToken();
    }
  }

  @Override
  @Nonnull
  public Reference createReference(
      @Nonnull String name, @Nonnull ObjId pointer, @Nullable ObjId extendedInfoObj)
      throws RefAlreadyExistsException, RetryTimeoutException {
    long refCreatedTimestamp = persist.config().currentTimeMicros();
    return createReferenceInternal(name, pointer, extendedInfoObj, refCreatedTimestamp);
  }

  @Override
  @Nonnull
  public Reference createReferenceForImport(
      @Nonnull String name,
      @Nonnull ObjId pointer,
      @Nullable ObjId extendedInfoObj,
      long createdAtMicros)
      throws RefAlreadyExistsException, RetryTimeoutException {
    return createReferenceInternal(name, pointer, extendedInfoObj, createdAtMicros);
  }

  @Nonnull
  private Reference createReferenceInternal(
      @Nonnull String name,
      @Nonnull ObjId pointer,
      @Nullable ObjId extendedInfoObj,
      long refCreatedTimestamp)
      throws RefAlreadyExistsException, RetryTimeoutException {
    checkArgument(!isInternalReferenceName(name));

    while (true) {
      CommitReferenceResult commitToIndex =
          commitCreateReference(name, pointer, extendedInfoObj, refCreatedTimestamp);
      Reference created = commitToIndex.created;
      Reference existing;

      LOGGER.debug(
          "Committed create reference {} with outcome {}, existing is {}",
          created,
          commitToIndex.kind,
          commitToIndex.existing);

      switch (commitToIndex.kind) {
        case ADDED_TO_INDEX:
          checkState(!created.deleted(), "internal error");
          try {
            return persist.addReference(created);
          } catch (RefAlreadyExistsException e) {
            // Reference recovery logic might have kicked in and added the reference. It that's the
            // case, just return it.
            existing = e.reference();
            if (Objects.equals(created, existing)) {
              break;
            }
            // Might happen in a rare race
            throw e;
          }
        case REF_ROW_MISSING:
          existing = commitToIndex.existing;
          checkState(!existing.deleted(), "internal error");
          // Note: addReference() may or may not throw a ReferenceAlreadyExistsException
          existing = persist.addReference(existing);
          throw new RefAlreadyExistsException(existing);
        case REF_ROW_EXISTS:
          // Reference recovery logic might have kicked in and added the reference. It that's the
          // case, just return it.
          existing = commitToIndex.existing;
          if (created.equals(existing)) {
            return existing;
          }
          if (!existing.deleted()) {
            throw new RefAlreadyExistsException(existing);
          }
          maybeRecover(name, existing, createRefsIndexSupplier());
          // try again
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public void deleteReference(@Nonnull String name, @Nonnull ObjId expectedPointer)
      throws RefNotFoundException, RefConditionFailedException, RetryTimeoutException {
    checkArgument(!isInternalReferenceName(name));

    Reference reference = persist.fetchReferenceForUpdate(name);
    Supplier<SuppliedCommitIndex> indexSupplier = null;
    if (reference == null) {
      StoreKey nameKey = key(name);
      indexSupplier = createRefsIndexSupplier();
      StoreIndexElement<CommitOp> index = indexSupplier.get().index().get(nameKey);
      if (index == null) {
        // not there --> okay
        throw new RefNotFoundException(reference(name, expectedPointer, false, 0, null));
      }
      reference = maybeRecover(name, null, indexSupplier);

      if (reference == null) {
        // not there, even after recovery
        throw new RefNotFoundException(reference(name, expectedPointer, false, 0, null));
      }
    }

    boolean actAsAlreadyDeleted = reference.deleted();

    if (!reference.pointer().equals(expectedPointer)) {
      // A previous deleteReference failed, act as if the first one succeeded, therefore this
      // one must throw a ReferenceNotFoundException instead of a ReferenceConditionFailedException
      if (!actAsAlreadyDeleted) {
        if (indexSupplier == null) {
          indexSupplier = createRefsIndexSupplier();
        }
        Reference recovered = maybeRecover(name, reference, indexSupplier);
        throw new RefConditionFailedException(recovered != null ? recovered : reference);
      }
    }

    if (!actAsAlreadyDeleted) {
      persist.markReferenceAsDeleted(reference);
      LOGGER.debug("Reference {} marked as deleted", reference);
    }

    LOGGER.debug("Commit deleted reference {}", reference);
    commitDeleteReference(reference, null);
    LOGGER.debug("Committed deleted reference {}", reference);

    try {
      persist.purgeReference(reference);
      LOGGER.debug("Reference {} purged", reference);
    } catch (RefNotFoundException ignore) {
      // deleted via "deletion recovery" - from another thread/process
    }

    if (actAsAlreadyDeleted) {
      // A previous deleteReference failed, act as if the first one succeeded, therefore this
      // one would have not found the reference.
      throw new RefNotFoundException(
          reference(
              name,
              expectedPointer,
              false,
              reference.createdAtMicros(),
              reference.extendedInfoObj(),
              reference.previousPointers()));
    }
  }

  static final class CommitReferenceResult {
    final Reference created;
    final Reference existing;
    final Kind kind;

    CommitReferenceResult(Reference reference, Reference existing, Kind kind) {
      this.created = reference;
      this.existing = existing;
      this.kind = kind;
    }

    enum Kind {
      ADDED_TO_INDEX,
      REF_ROW_EXISTS,
      REF_ROW_MISSING
    }

    @Override
    public String toString() {
      return "CommitReferenceResult{"
          + "created="
          + created
          + ", existing="
          + existing
          + ", kind="
          + kind
          + '}';
    }
  }

  @VisibleForTesting // needed to simulate recovery scenarios
  CommitReferenceResult commitCreateReference(
      String name, ObjId pointer, ObjId extendedInfoObj, long refCreatedTimestamp)
      throws RetryTimeoutException {
    Reference reference = reference(name, pointer, false, refCreatedTimestamp, extendedInfoObj);
    try {
      return commitRetry(
          persist,
          (p, retryState) -> {
            Reference refRefs = requireNonNull(p.fetchReferenceForUpdate(REF_REFS.name()));
            RefObj ref = ref(name, pointer, refCreatedTimestamp, extendedInfoObj);
            try {
              p.storeObj(ref);
            } catch (ObjTooLargeException e) {
              throw new RuntimeException(e);
            }

            StoreKey k = key(name);

            Instant now = persist.config().clock().instant();
            CreateCommit c =
                newCommitBuilder()
                    .message("Create reference " + name + " pointing to " + pointer)
                    .parentCommitId(refRefs.pointer())
                    .addAdds(commitAdd(k, 0, requireNonNull(ref.id()), null, null))
                    .headers(
                        newCommitHeaders()
                            .add("operation", "create")
                            .add("name", name)
                            .add("head", pointer.toString())
                            .add("timestamp", now.toString())
                            .add("timestamp.millis", Long.toString(now.toEpochMilli()))
                            .build())
                    .commitType(CommitType.INTERNAL)
                    .build();

            commitReferenceChange(p, refRefs, c);

            return new CommitReferenceResult(reference, null, ADDED_TO_INDEX);
          });
    } catch (CommitConflictException e) {
      checkState(e.conflicts().size() == 1, "Unexpected amount of conflicts %s", e.conflicts());

      CommitConflict conflict = e.conflicts().get(0);
      checkState(conflict.conflictType() == KEY_EXISTS, "Unexpected conflict type %s", conflict);

      Supplier<SuppliedCommitIndex> indexSupplier = createRefsIndexSupplier();
      StoreIndexElement<CommitOp> el = indexSupplier.get().index().get(key(name));
      checkNotNull(el, "Key %s missing in index", name);

      Reference existing = persist.fetchReferenceForUpdate(name);

      if (existing != null) {
        return new CommitReferenceResult(reference, existing, REF_ROW_EXISTS);
      }

      RefObj ref;
      try {
        ref =
            persist.fetchTypedObj(
                requireNonNull(el.content().value(), "Reference commit operation has no value"),
                REF,
                RefObj.class);
      } catch (ObjNotFoundException ex) {
        throw new RuntimeException("Internal error getting reference creation object", e);
      }
      return new CommitReferenceResult(
          reference,
          reference(
              name, ref.initialPointer(), false, ref.createdAtMicros(), ref.extendedInfoObj()),
          REF_ROW_MISSING);
    } catch (CommitWrappedException e) {
      throw new RuntimeException(
          format(
              "An unexpected internal error happened while committing the creation of the reference '%s'",
              reference),
          e.getCause());
    }
  }

  @VisibleForTesting // needed to simulate recovery scenarios
  // Note: commitForReference is for testing, to test race conditions
  void commitDeleteReference(Reference reference, ObjId expectedRefRefsHead)
      throws RetryTimeoutException {
    try {
      commitRetry(
          persist,
          (p, retryState) -> {
            Reference refRefs = requireNonNull(p.fetchReferenceForUpdate(REF_REFS.name()));
            if (expectedRefRefsHead != null && !refRefs.pointer().equals(expectedRefRefsHead)) {
              throw new RuntimeException(REF_REFS_ADVANCED);
            }

            CommitObj commit;
            try {
              commit = p.fetchTypedObj(refRefs.pointer(), COMMIT, CommitObj.class);
            } catch (ObjNotFoundException e) {
              throw new RuntimeException("Internal error getting reference creation log commit", e);
            }
            StoreIndex<CommitOp> index = indexesLogic(persist).buildCompleteIndexOrEmpty(commit);

            StoreKey key = key(reference.name());

            StoreIndexElement<CommitOp> indexElement = index.get(key);
            if (indexElement != null) {
              CommitOp indexElementContent = indexElement.content();
              if (indexElementContent.action().exists()) {
                Instant now = persist.config().clock().instant();
                CreateCommit c =
                    newCommitBuilder()
                        .message(
                            "Drop reference "
                                + reference.name()
                                + " pointing to "
                                + reference.pointer())
                        .parentCommitId(refRefs.pointer())
                        .addRemoves(
                            commitRemove(
                                key,
                                0,
                                requireNonNull(indexElementContent.value()),
                                indexElementContent.contentId()))
                        .headers(
                            newCommitHeaders()
                                .add("operation", "delete")
                                .add("name", reference.name())
                                .add("head", reference.pointer().toString())
                                .add("timestamp", now.toString())
                                .add("timestamp.millis", Long.toString(now.toEpochMilli()))
                                .build())
                        .commitType(CommitType.INTERNAL)
                        .build();

                commitReferenceChange(p, refRefs, c);
              }
            }

            return null;
          });
    } catch (CommitConflictException e) {
      throw new RuntimeException(
          format(
              "An unexpected internal error happened while committing the deletion of the reference '%s'",
              reference),
          e);
    } catch (CommitWrappedException e) {
      throw new RuntimeException(
          format(
              "An unexpected internal error happened while committing the deletion of the reference '%s'",
              reference),
          e.getCause());
    }
  }

  private static void commitReferenceChange(Persist p, Reference refRefs, CreateCommit c)
      throws CommitConflictException, RetryException {
    CommitObj commit;
    try {
      commit = commitLogic(p).doCommit(c, emptyList());
    } catch (ObjNotFoundException e) {
      throw new RuntimeException("Internal error committing to log of references", e);
    }

    checkState(commit != null);

    // Commit to REF_REFS
    try {
      p.updateReferencePointer(refRefs, commit.id());
    } catch (RefConditionFailedException e) {
      throw new RetryException();
    } catch (RefNotFoundException e) {
      throw new RuntimeException("Internal reference not found", e);
    }
  }

  @Override
  @Nonnull
  public Reference assignReference(@Nonnull Reference current, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    checkArgument(!current.isInternal());

    return persist.updateReferencePointer(current, newPointer);
  }

  @Override
  public Reference rewriteCommitLog(
      @Nonnull Reference current, BiPredicate<Integer, CommitObj> cutoffPredicate)
      throws RefNotFoundException,
          RefConditionFailedException,
          CommitConflictException,
          ObjNotFoundException {
    var commitLogic = commitLogic(persist);
    var indexesLogic = indexesLogic(persist);

    var log = commitLogic.commitLog(CommitLogQuery.commitLogQuery(current.pointer()));
    var commits = new ArrayDeque<CommitObj>();
    for (int n = 1; log.hasNext(); n++) {
      var commit = log.next();

      if (cutoffPredicate.test(n, commit)) {
        // THIS is the last commit to retain

        if (commit.directParent().equals(EMPTY_OBJ_ID)) {
          return current;
        }

        // Build the "oldest" commit
        var c =
            newCommitBuilder()
                .message(commit.message())
                .parentCommitId(EMPTY_OBJ_ID)
                .headers(commit.headers());
        for (StoreIndexElement<CommitOp> element : indexesLogic.buildCompleteIndexOrEmpty(commit)) {
          var op = element.content();
          if (op.action().exists()) {
            c.addAdds(commitAdd(element.key(), op.payload(), op.value(), null, op.contentId()));
          }
        }
        var head = commitLogic.buildCommitObj(c.build());
        commitLogic.storeCommit(head, List.of());

        // Build the "next newer" commit(s)
        while (!commits.isEmpty()) {
          commit = commits.removeLast();

          c =
              newCommitBuilder()
                  .message(commit.message())
                  .parentCommitId(head.id())
                  .headers(commit.headers());
          for (StoreIndexElement<CommitOp> element :
              indexesLogic.incrementalIndexFromCommit(commit)) {
            var op = element.content();
            if (op.action().currentCommit()) {
              if (op.action().exists()) {
                c.addAdds(commitAdd(element.key(), op.payload(), op.value(), null, op.contentId()));
              } else {
                c.addRemoves(commitRemove(element.key(), op.payload(), op.value(), op.contentId()));
              }
            }
          }
          head = commitLogic.buildCommitObj(c.build());
          commitLogic.storeCommit(head, List.of());
        }

        return persist.updateReferencePointer(current, head.id());
      }

      // memoize the
      commits.addLast(commit);
    }

    // no cut-off found, just return "current"
    return current;
  }

  private Reference maybeRecover(
      @Nonnull String name,
      Reference ref,
      @Nonnull Supplier<SuppliedCommitIndex> refsIndexSupplier) {
    if (ref == null) {
      SuppliedCommitIndex suppliedIndex = refsIndexSupplier.get();

      StoreIndexElement<CommitOp> commitOp = suppliedIndex.index().get(key(name));

      if (commitOp == null) {
        // Reference not in index - nothing to do.
        return null;
      }

      CommitOp commitOpContent = commitOp.content();
      if (commitOpContent.action().exists()) {
        // The reference is present in InternalRefs.REF_REFS as an existing key, but not via
        // Persist.findReference --> resume reference creation.
        RefObj initialRef;
        try {
          initialRef =
              persist.fetchTypedObj(
                  requireNonNull(
                      commitOpContent.value(), "Reference commit operation has no value"),
                  REF,
                  RefObj.class);
        } catch (ObjNotFoundException e) {
          throw new RuntimeException("Internal error getting reference creation object", e);
        }
        ref =
            reference(
                name,
                initialRef.initialPointer(),
                false,
                initialRef.createdAtMicros(),
                initialRef.extendedInfoObj());
        try {
          if (refRefsOutOfDate(suppliedIndex)) {
            return null;
          }

          LOGGER.debug(
              "Recovering reference creation {} from commit op {}",
              ref,
              commitOp.content().action());
          ref = persist.addReference(ref);
          LOGGER.debug(
              "Recovered reference creation {} from commit op {}",
              ref,
              commitOp.content().action());
        } catch (RefAlreadyExistsException e) {
          ref = e.reference();
        }
        return ref;
      } else {
        // Reference committed to int/refs as deleted, it's gone
        return null;
      }

    } else if (ref.deleted()) {
      SuppliedCommitIndex suppliedIndex = refsIndexSupplier.get();

      StoreIndexElement<CommitOp> commitOp = suppliedIndex.index().get(key(name));

      if (commitOp == null) {
        throw new RuntimeException("Loaded Reference is marked as deleted, but not found in index");
      }

      CommitOp commitOpContent = commitOp.content();
      if (commitOpContent.action().exists()) {
        try {
          if (refRefsOutOfDate(suppliedIndex)) {
            return ref;
          }

          LOGGER.debug(
              "Recovering reference deletion commit for {} from commit op {}",
              ref,
              commitOp.content().action());
          commitDeleteReference(ref, suppliedIndex.pointer());
          LOGGER.debug(
              "Recovered reference deletion commit for {} from commit op {}",
              ref,
              commitOp.content().action());

          persist.purgeReference(ref);
        } catch (RefNotFoundException | RefConditionFailedException e) {
          // ignore
        } catch (RetryTimeoutException e) {
          LOGGER.debug(
              "Recovery of reference deletion commit-retry failed for {} from commit op {}",
              ref,
              commitOp.content().action(),
              e);
          throw new RuntimeException(e);
        } catch (RuntimeException e) {
          if (REF_REFS_ADVANCED.equals(e.getMessage())) {
            // It is expected that int/refs can advance while a reference is being recovered.
            // This can happen, just ignore the recovery, it will be retried in the future, if
            // necessary.
            return ref;
          }
          throw e;
        }
      } else {
        // Reference marked as deleted in index, purge it.
        try {
          LOGGER.debug(
              "Recovering reference purge for {} from commit op {}",
              ref,
              commitOp.content().action());
          persist.purgeReference(ref);
        } catch (RefNotFoundException | RefConditionFailedException e) {
          LOGGER.debug(
              "Recovery of reference purge for {} from commit op {} failed",
              ref,
              commitOp.content().action(),
              e);
          // ignore
        }
      }
      return null;
    }

    return ref;
  }

  private boolean refRefsOutOfDate(SuppliedCommitIndex index) {
    Reference refRefs = persist.fetchReferenceForUpdate(REF_REFS.name());
    return !index.pointer().equals(requireNonNull(refRefs, "internal refs missing").pointer());
  }

  @VisibleForTesting
  Supplier<SuppliedCommitIndex> createRefsIndexSupplier() {
    return indexesLogic(persist)
        .createIndexSupplier(
            () -> {
              Reference ref = persist.fetchReference(REF_REFS.name());
              return ref != null ? ref.pointer() : EMPTY_OBJ_ID;
            });
  }

  @VisibleForTesting
  Supplier<SuppliedCommitIndex> createRefsIndexSupplier(Reference refRefs) {
    return indexesLogic(persist)
        .createIndexSupplier(() -> refRefs != null ? refRefs.pointer() : EMPTY_OBJ_ID);
  }
}
