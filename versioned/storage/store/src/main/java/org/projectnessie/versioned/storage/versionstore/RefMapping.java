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
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.COMMIT_TIME;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.headerValueToInstant;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.RelativeCommitSpec;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitConflict;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

/**
 * Maps {@link org.projectnessie.versioned.Ref} from and to {@link
 * org.projectnessie.versioned.storage.common.persist.Reference reference names}.
 */
public class RefMapping {

  public static final String REFS = "refs/";
  public static final String REFS_TAGS = REFS + "tags/";
  public static final String REFS_HEADS = REFS + "heads/";
  public static final Hash NO_ANCESTOR = objIdToHash(EMPTY_OBJ_ID);

  private final Persist persist;

  public RefMapping(Persist persist) {
    this.persist = persist;
  }

  public static ReferenceNotFoundException referenceNotFound(String ref) {
    return new ReferenceNotFoundException(format("Named reference '%s' not found", ref));
  }

  public static ReferenceNotFoundException referenceNotFound(NamedRef ref) {
    return referenceNotFound(ref.getName());
  }

  public static ReferenceAlreadyExistsException referenceAlreadyExists(NamedRef ref) {
    return new ReferenceAlreadyExistsException(
        format("Named reference '%s' already exists.", ref.getName()));
  }

  public static ReferenceNotFoundException hashNotFound(NamedRef ref, Hash hash) {
    return new ReferenceNotFoundException(
        format("Could not find commit '%s' in reference '%s'.", hash.asString(), ref.getName()));
  }

  public static ReferenceNotFoundException hashNotFound(Hash hash) {
    return new ReferenceNotFoundException(format("Commit '%s' not found", hash.asString()));
  }

  public static ReferenceNotFoundException objectNotFound(ObjId hash, Throwable cause) {
    return new ReferenceNotFoundException(format("Commit '%s' not found", hash), cause);
  }

  public static ReferenceNotFoundException objectNotFound(ObjNotFoundException e) {
    List<ObjId> ids = e.objIds();
    if (ids.size() == 1) {
      return objectNotFound(e.objIds().get(0), e);
    }

    return new ReferenceNotFoundException(
        format(
            "Could not find objects %s.",
            ids.stream().map(o -> "'" + o + "'").collect(Collectors.joining(", "))));
  }

  public static ReferenceNotFoundException referenceNotFound(ObjNotFoundException e) {
    List<ObjId> ids = e.objIds();
    if (ids.size() == 1) {
      return objectNotFound(e.objIds().get(0), e);
    }

    return new ReferenceNotFoundException(
        format(
            "Could not find commits %s.",
            ids.stream().map(o -> "'" + o + "'").collect(Collectors.joining(", "))));
  }

  public static ReferenceConflictException referenceConflictException(CommitConflictException e) {
    return referenceConflictException(e.conflicts());
  }

  public static ReferenceConflictException referenceConflictException(
      List<CommitConflict> conflicts) {
    return new ReferenceConflictException(
        conflicts.stream().map(RefMapping::commitConflictToConflict).collect(Collectors.toList()));
  }

  public static Conflict commitConflictToConflict(CommitConflict conflict) {
    ContentKey key = storeKeyToKey(conflict.key());
    String k = key != null ? "key '" + key + "'" : "store-key '" + conflict.key() + "'";
    String msg;
    ConflictType conflictType;
    switch (conflict.conflictType()) {
      case KEY_DOES_NOT_EXIST:
        conflictType = ConflictType.KEY_DOES_NOT_EXIST;
        msg = k + " does not exist";
        break;
      case KEY_EXISTS:
        conflictType = ConflictType.KEY_EXISTS;
        msg = k + " already exists";
        break;
      case PAYLOAD_DIFFERS:
        conflictType = ConflictType.PAYLOAD_DIFFERS;
        msg = "payload of existing and expected content for " + k + " are different";
        break;
      case CONTENT_ID_DIFFERS:
        conflictType = ConflictType.CONTENT_ID_DIFFERS;
        msg = "content IDs of existing and expected content for " + k + " are different";
        break;
      case VALUE_DIFFERS:
        conflictType = ConflictType.VALUE_DIFFERS;
        msg = "values of existing and expected content for " + k + " are different";
        break;
      default:
        conflictType = ConflictType.UNKNOWN;
        msg = conflict.toString();
    }
    return conflict(conflictType, key, msg);
  }

  public static ReferenceConflictException referenceConflictException(
      NamedRef ref, Hash expected, ObjId currentHeadCommit) {
    return new ReferenceConflictException(
        conflict(
            ConflictType.UNEXPECTED_HASH,
            null,
            format(
                "named-reference '%s' is not at expected hash '%s', but at '%s'",
                ref.getName(), expected.asString(), currentHeadCommit)));
  }

  public static void verifyExpectedHash(
      Hash referenceCurrentHead, NamedRef reference, Hash expectedHead)
      throws ReferenceConflictException {
    if (!referenceCurrentHead.equals(expectedHead)) {
      throw referenceConflictException(reference, expectedHead, hashToObjId(referenceCurrentHead));
    }
  }

  @Nonnull
  public static NamedRef referenceToNamedRef(@Nonnull Reference reference) {
    return referenceToNamedRef(reference.name());
  }

  @Nonnull
  public static NamedRef referenceToNamedRef(@Nonnull String name) {
    if (name.startsWith(REFS_HEADS)) {
      checkArgument(name.length() > REFS_HEADS.length());
      return BranchName.of(name.substring(REFS_HEADS.length()));
    }
    if (name.startsWith(REFS_TAGS)) {
      checkArgument(name.length() > REFS_TAGS.length());
      return TagName.of(name.substring(REFS_TAGS.length()));
    }
    throw new IllegalArgumentException(
        "Must be a " + REFS_HEADS + " or " + REFS_TAGS + ", but got " + name);
  }

  public static String namedRefToRefName(NamedRef ref) {
    if (ref instanceof BranchName) {
      return asBranchName(ref.getName());
    }
    if (ref instanceof TagName) {
      return asTagName(ref.getName());
    }
    throw new IllegalArgumentException("Argument must be a branch or tag, but was " + ref);
  }

  public static String asTagName(String name) {
    return REFS_TAGS + name;
  }

  public static String asBranchName(String name) {
    return REFS_HEADS + name;
  }

  CommitObj resolveRefHead(@Nonnull Ref ref) throws ReferenceNotFoundException {
    if (ref instanceof NamedRef) {
      NamedRef namedRef = (NamedRef) ref;
      return resolveNamedRefHead(namedRef);
    }
    if (ref instanceof Hash) {
      Hash hash = (Hash) ref;
      CommitLogic commitLogic = commitLogic(persist);
      try {
        return commitLogic.fetchCommit(hashToObjId(hash));
      } catch (ObjNotFoundException e) {
        throw referenceNotFound(e);
      }
    }
    // hard to get here, detached caught in namedRefToRefName()
    throw new IllegalArgumentException("Unsupported ref type, got " + ref);
  }

  CommitObj resolveRefHeadForUpdate(@Nonnull NamedRef namedRef) throws ReferenceNotFoundException {
    return resolveNamedRefHead(resolveNamedRefForUpdate(namedRef));
  }

  CommitObj resolveNamedRefHead(@Nonnull NamedRef namedRef) throws ReferenceNotFoundException {
    return resolveNamedRefHead(resolveNamedRef(namedRef));
  }

  CommitObj resolveNamedRefHead(@Nonnull Reference reference) throws ReferenceNotFoundException {
    try {
      return commitLogic(persist).headCommit(reference);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  @Nonnull
  public Reference resolveNamedRef(@Nonnull NamedRef namedRef) throws ReferenceNotFoundException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);
    try {
      return referenceLogic.getReference(refName);
    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    }
  }

  @Nonnull
  public Reference resolveNamedRefForUpdate(@Nonnull NamedRef namedRef)
      throws ReferenceNotFoundException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);
    try {
      return referenceLogic.getReferenceForUpdate(refName);
    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    }
  }

  public Reference resolveNamedRef(@Nonnull String refName) throws ReferenceNotFoundException {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    List<Reference> refs =
        referenceLogic.getReferences(asList(asBranchName(refName), asTagName(refName)));
    Reference branch = refs.get(0);
    Reference tag = refs.get(1);

    // Prefer branches
    if (branch != null) {
      return branch;
    }

    if (tag != null) {
      return tag;
    }

    throw referenceNotFound(refName);
  }

  public CommitObj commitInChain(
      NamedRef namedRef,
      CommitObj startCommit,
      Optional<Hash> hashOnReference,
      List<RelativeCommitSpec> relativeLookups)
      throws ReferenceNotFoundException {
    ObjId commitId = startCommit != null ? startCommit.id() : EMPTY_OBJ_ID;

    if (hashOnReference.isPresent()) {
      Hash hash = hashOnReference.get();
      if (NO_ANCESTOR.equals(hash)) {
        return null;
      }

      startCommit = commitInChain(commitId, hashToObjId(hash));
      if (startCommit == null) {
        throw hashNotFound(namedRef, hash);
      }
    }

    startCommit = relativeSpec(startCommit, relativeLookups);

    return startCommit;
  }

  @VisibleForTesting
  CommitObj relativeSpec(CommitObj startCommit, List<RelativeCommitSpec> relativespecs)
      throws ReferenceNotFoundException {
    CommitLogic commitLogic = commitLogic(persist);
    for (RelativeCommitSpec spec : relativespecs) {
      if (startCommit == null) {
        break;
      }

      switch (spec.type()) {
        case TIMESTAMP_MILLIS_EPOCH:
          startCommit = findWithSmallerTimestamp(startCommit, commitLogic, spec.instantValue());
          break;
        case N_TH_PREDECESSOR:
          startCommit = findNthPredecessor(startCommit, commitLogic, (int) spec.longValue());
          break;
        case N_TH_PARENT:
          startCommit = findNthParent(startCommit, commitLogic, (int) spec.longValue());
          break;
        default:
          throw new IllegalArgumentException("Unknown lookup type " + spec.type());
      }
    }
    return startCommit;
  }

  @Nullable
  private static CommitObj findWithSmallerTimestamp(
      CommitObj startCommit, CommitLogic commitLogic, Instant timestampMillisEpoch) {
    if (createdTimestampMatches(startCommit, timestampMillisEpoch)) {
      return startCommit;
    }
    PagedResult<CommitObj, ObjId> log =
        commitLogic.commitLog(commitLogQuery(startCommit.directParent()));
    while (log.hasNext()) {
      CommitObj commit = log.next();
      if (createdTimestampMatches(commit, timestampMillisEpoch)) {
        return commit;
      }
    }
    return null;
  }

  @VisibleForTesting
  static boolean createdTimestampMatches(CommitObj commit, Instant timestampMillisEpoch) {
    Instant commitCreated = commitCreatedTimestamp(commit);
    return commitCreated.compareTo(timestampMillisEpoch) <= 0;
  }

  @VisibleForTesting
  static Instant commitCreatedTimestamp(CommitObj commit) {
    String hdr = commit.headers().getFirst(COMMIT_TIME);
    Instant commitCreated = null;
    if (hdr != null) {
      try {
        commitCreated = headerValueToInstant(hdr);
      } catch (Exception ignore) {
        // ignore
      }
    }
    if (commitCreated == null) {
      long created = commit.created();
      long seconds = MICROSECONDS.toSeconds(created);
      long nanos = MICROSECONDS.toNanos(created) % SECONDS.toNanos(1);
      commitCreated = Instant.ofEpochSecond(seconds, nanos);
    }
    return commitCreated;
  }

  @Nullable
  private static CommitObj findNthParent(
      CommitObj startCommit, CommitLogic commitLogic, int nthParent)
      throws ReferenceNotFoundException {
    ObjId id;
    if (nthParent == 1) {
      id = startCommit.directParent();
    } else {
      List<ObjId> secondaryParents = startCommit.secondaryParents();
      int secondary = nthParent - 2;
      if (secondaryParents.size() <= secondary) {
        return null;
      }
      id = secondaryParents.get(secondary);
    }
    try {
      return commitLogic.fetchCommit(id);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  @Nullable
  private static CommitObj findNthPredecessor(
      CommitObj startCommit, CommitLogic commitLogic, int nthPredecessor)
      throws ReferenceNotFoundException {
    PagedResult<ObjId, ObjId> log =
        commitLogic.commitIdLog(commitLogQuery(startCommit.directParent()));
    while (log.hasNext()) {
      ObjId id = log.next();
      if (--nthPredecessor == 0) {
        try {
          return commitLogic.fetchCommit(id);
        } catch (ObjNotFoundException e) {
          throw referenceNotFound(e);
        }
      }
    }
    return null;
  }

  public CommitObj commitInChain(ObjId commitId, ObjId verifyId) throws ReferenceNotFoundException {
    CommitLogic commitLogic = commitLogic(persist);
    PagedResult<ObjId, ObjId> log = commitLogic.commitIdLog(commitLogQuery(commitId));
    while (log.hasNext()) {
      ObjId current = log.next();
      if (verifyId.equals(current)) {
        try {
          return commitLogic.fetchCommit(current);
        } catch (ObjNotFoundException e) {
          throw referenceNotFound(e);
        }
      }
    }
    return null;
  }
}
