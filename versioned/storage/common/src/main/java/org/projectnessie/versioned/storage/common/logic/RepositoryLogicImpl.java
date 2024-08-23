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

import static java.time.OffsetDateTime.now;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.CommitRetry.commitRetry;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.KEY_REPO_DESCRIPTION;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REFS;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REPO;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.stringLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.common.util.Ser.SHARED_OBJECT_MAPPER;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
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
import org.projectnessie.versioned.storage.common.logic.CommitRetry.RetryException;
import org.projectnessie.versioned.storage.common.logic.StringLogic.StringValue;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

/** Logic to setup/initialize a Nessie repository. */
final class RepositoryLogicImpl implements RepositoryLogic {

  static final String REFS_HEADS = "refs/heads/";
  private final Persist persist;

  RepositoryLogicImpl(Persist persist) {
    this.persist = persist;
  }

  @Override
  public void initialize(@Nonnull String defaultBranchName) {
    initialize(defaultBranchName, true, b -> {});
  }

  @Override
  public void initialize(
      @Nonnull String defaultBranchName,
      boolean createDefaultBranch,
      Consumer<RepositoryDescription.Builder> repositoryDescription) {
    initializeInternalRef(REF_REFS, b -> {});

    if (createDefaultBranch) {
      try {
        referenceLogic(persist).createReference(REFS_HEADS + defaultBranchName, EMPTY_OBJ_ID, null);
      } catch (RefAlreadyExistsException ignore) {
        // ignore an existing reference (theoretically possible race of two initialize() calls)
      } catch (RetryTimeoutException e) {
        throw new RuntimeException(e);
      }
    }

    initializeInternalRef(
        REF_REPO, b -> addRepositoryDescription(b, repositoryDescription, defaultBranchName));
  }

  @Override
  public boolean repositoryExists() {
    try {
      Reference ref = persist.fetchReferenceForUpdate(REF_REPO.name());
      if (ref == null) {
        return false;
      }

      CommitObj repoDescCommit = commitLogic(persist).fetchCommit(ref.pointer());
      return repoDescCommit != null;
    } catch (ObjNotFoundException e) {
      return false;
    }
  }

  @Nullable
  @Override
  public RepositoryDescription fetchRepositoryDescription() {
    try {
      Reference ref = persist.fetchReference(REF_REPO.name());
      if (ref == null) {
        return null;
      }

      CommitObj repoDescCommit = commitLogic(persist).fetchCommit(ref.pointer());
      if (repoDescCommit == null) {
        return null;
      }

      StoreIndex<CommitOp> index =
          indexesLogic(persist).buildCompleteIndex(repoDescCommit, Optional.empty());
      StoreIndexElement<CommitOp> el = index.get(KEY_REPO_DESCRIPTION);
      if (el == null) {
        return null;
      }
      CommitOp op = el.content();
      if (!op.action().exists()) {
        return null;
      }

      StringValue value =
          stringLogic(persist)
              .fetchString(
                  requireNonNull(
                      op.value(), "Commit operation for repository description has no value"));

      return deserialize(value);
    } catch (ObjNotFoundException e) {
      return null;
    }
  }

  private static RepositoryDescription deserialize(StringValue value) {
    try {
      return SHARED_OBJECT_MAPPER.readValue(value.completeValue(), RepositoryDescription.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] serialize(RepositoryDescription repositoryDescription) {
    try {
      return SHARED_OBJECT_MAPPER.writeValueAsBytes(repositoryDescription);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void addRepositoryDescription(
      CreateCommit.Builder b,
      Consumer<RepositoryDescription.Builder> repositoryDescription,
      String defaultBranchName) {
    try {
      Instant now = Instant.now(persist.config().clock());
      RepositoryDescription.Builder repoDesc =
          RepositoryDescription.builder()
              .oldestPossibleCommitTime(now)
              .repositoryCreatedTime(now)
              .defaultBranchName(defaultBranchName);

      repositoryDescription.accept(repoDesc);

      StringObj string =
          stringLogic(persist)
              .updateString(
                  null,
                  "application/json",
                  SHARED_OBJECT_MAPPER.writeValueAsBytes(repoDesc.build()));
      // can safely ignore the response from storeObj() - it's fine, if the obj already exists
      persist.storeObj(string);
      b.addAdds(commitAdd(KEY_REPO_DESCRIPTION, 0, requireNonNull(string.id()), null, null));
    } catch (ObjTooLargeException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RepositoryDescription updateRepositoryDescription(RepositoryDescription newDescription)
      throws RetryTimeoutException {
    // prevent modification of read-only attributes
    RepositoryDescription existingDescription = requireNonNull(fetchRepositoryDescription());
    RepositoryDescription sanitizedDescription =
        ImmutableRepositoryDescription.builder()
            .from(newDescription)
            .oldestPossibleCommitTime(existingDescription.oldestPossibleCommitTime())
            .repositoryCreatedTime(existingDescription.repositoryCreatedTime())
            .build();
    byte[] serialized = serialize(sanitizedDescription);
    try {
      StringValue existing =
          commitRetry(
              persist,
              (p, retryState) -> {
                try {
                  Reference reference =
                      requireNonNull(persist.fetchReferenceForUpdate(REF_REPO.name()));
                  return stringLogic(persist)
                      .updateStringOnRef(
                          reference,
                          KEY_REPO_DESCRIPTION,
                          b ->
                              b.message("Update repository description")
                                  .commitType(CommitType.INTERNAL),
                          "application/json",
                          serialized);
                } catch (RefConditionFailedException | CommitConflictException e) {
                  throw new RetryException();
                } catch (ObjNotFoundException | RefNotFoundException e) {
                  throw new CommitWrappedException(e);
                }
              });
      return existing != null ? deserialize(existing) : null;
    } catch (CommitConflictException e) {
      throw new RuntimeException(
          "An unexpected internal error happened while committing a repository description update");
    } catch (CommitWrappedException e) {
      throw new RuntimeException(
          "An unexpected internal error happened while committing a repository description update",
          e.getCause());
    }
  }

  @SuppressWarnings({"JavaTimeDefaultTimeZone"})
  private void initializeInternalRef(
      InternalRef internalRef, Consumer<CreateCommit.Builder> commitEnhancer) {
    Reference reference = persist.fetchReferenceForUpdate(internalRef.name());

    if (reference == null) {
      CreateCommit.Builder c =
          newCommitBuilder()
              .parentCommitId(EMPTY_OBJ_ID)
              .headers(
                  CommitHeaders.newCommitHeaders()
                      .add("repo.id", persist.config().repositoryId())
                      .add("timestamp", now())
                      .build())
              .message("Initialize reference " + internalRef.name())
              .commitType(CommitType.INTERNAL);

      commitEnhancer.accept(c);

      CommitLogic commitLogic = commitLogic(persist);
      CommitObj commit;
      try {
        commit = commitLogic.doCommit(c.build(), emptyList());
      } catch (CommitConflictException | ObjNotFoundException e) {
        throw new RuntimeException(e);
      }

      try {
        persist.addReference(
            reference(
                internalRef.name(),
                commit.id(),
                false,
                persist.config().currentTimeMicros(),
                null));
      } catch (RefAlreadyExistsException ignore) {
        // ignore an existing reference (theoretically possible race of two initialize() calls)
      }
    }
  }
}
