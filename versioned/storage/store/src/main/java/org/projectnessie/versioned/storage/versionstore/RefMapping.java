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
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
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

  public static ReferenceNotFoundException objectNotFound(ObjId hash) {
    return new ReferenceNotFoundException(format("Commit '%s' not found", hash));
  }

  public static ReferenceNotFoundException objectNotFound(ObjNotFoundException e) {
    List<ObjId> ids = e.objIds();
    if (ids.size() == 1) {
      return objectNotFound(e.objIds().get(0));
    }

    return new ReferenceNotFoundException(
        format(
            "Could not find objects %s.",
            ids.stream().map(o -> "'" + o + "'").collect(Collectors.joining(", "))));
  }

  public static ReferenceNotFoundException referenceNotFound(ObjNotFoundException e) {
    List<ObjId> ids = e.objIds();
    if (ids.size() == 1) {
      return objectNotFound(e.objIds().get(0));
    }

    return new ReferenceNotFoundException(
        format(
            "Could not find commits %s.",
            ids.stream().map(o -> "'" + o + "'").collect(Collectors.joining(", "))));
  }

  public static ReferenceConflictException referenceConflictException(CommitConflictException e) {
    return new ReferenceConflictException(
        e.conflicts().stream()
            .map(
                conflict -> {
                  ContentKey key = storeKeyToKey(conflict.key());
                  String k =
                      key != null ? "key '" + key + "'" : "store-key '" + conflict.key() + "'";
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
                      msg =
                          "content IDs of existing and expected content for "
                              + k
                              + " are different";
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
                })
            .collect(Collectors.toList()));
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
      Hash referenceCurrentHead, NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceConflictException {
    if (expectedHead.isPresent() && !referenceCurrentHead.equals(expectedHead.get())) {
      throw referenceConflictException(
          reference, expectedHead.get(), hashToObjId(referenceCurrentHead));
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public static NamedRef referenceToNamedRef(
      @Nonnull @jakarta.annotation.Nonnull Reference reference) {
    return referenceToNamedRef(reference.name());
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public static NamedRef referenceToNamedRef(@Nonnull @jakarta.annotation.Nonnull String name) {
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

  CommitObj resolveRefHead(@Nonnull @jakarta.annotation.Nonnull Ref ref)
      throws ReferenceNotFoundException {
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

  CommitObj resolveNamedRefHead(@Nonnull @jakarta.annotation.Nonnull NamedRef namedRef)
      throws ReferenceNotFoundException {
    return resolveNamedRefHead(resolveNamedRef(namedRef));
  }

  CommitObj resolveNamedRefHead(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws ReferenceNotFoundException {
    try {
      return commitLogic(persist).headCommit(reference);
    } catch (ObjNotFoundException e) {
      throw referenceNotFound(e);
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference resolveNamedRef(@Nonnull @jakarta.annotation.Nonnull NamedRef namedRef)
      throws ReferenceNotFoundException {
    String refName = namedRefToRefName(namedRef);
    ReferenceLogic referenceLogic = referenceLogic(persist);
    try {
      return referenceLogic.getReference(refName);
    } catch (RefNotFoundException e) {
      throw referenceNotFound(namedRef);
    }
  }

  public Reference resolveNamedRef(@Nonnull @jakarta.annotation.Nonnull String refName)
      throws ReferenceNotFoundException {
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
      NamedRef namedRef, CommitObj startCommit, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    ObjId commitId = startCommit != null ? startCommit.id() : EMPTY_OBJ_ID;

    if (hashOnReference.isPresent()) {
      Hash hash = hashOnReference.get();
      if (NO_ANCESTOR.equals(hash)) {
        return null;
      }
      ObjId verifyId = hashToObjId(hash);
      CommitObj current = commitInChain(commitId, verifyId);
      if (current != null) {
        return current;
      }
      throw hashNotFound(namedRef, hash);
    }

    return startCommit;
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
