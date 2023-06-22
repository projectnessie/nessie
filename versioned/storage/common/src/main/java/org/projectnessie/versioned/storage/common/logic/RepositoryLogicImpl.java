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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.KEY_REPO_DESCRIPTION;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REFS;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REPO;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjType.STRING;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.common.util.Ser.SHARED_OBJECT_MAPPER;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
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
  public void initialize(@Nonnull @jakarta.annotation.Nonnull String defaultBranchName) {
    initialize(defaultBranchName, true, b -> {});
  }

  @Override
  public void initialize(
      @Nonnull @jakarta.annotation.Nonnull String defaultBranchName,
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
      Reference ref = persist.fetchReference(REF_REPO.name());
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
  @jakarta.annotation.Nullable
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

      StringObj obj =
          persist.fetchTypedObj(
              requireNonNull(
                  op.value(), "Commit operation for repository description has no value"),
              STRING,
              StringObj.class);

      return readRepositoryDescription(obj);
    } catch (ObjNotFoundException e) {
      return null;
    }
  }

  private RepositoryDescription readRepositoryDescription(StringObj stringObj) {
    try {
      return SHARED_OBJECT_MAPPER.readValue(
          stringObj.text().toByteArray(), RepositoryDescription.class);
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

      ByteString text = unsafeWrap(SHARED_OBJECT_MAPPER.writeValueAsBytes(repoDesc.build()));

      StringObj string = stringData("application/json", Compression.NONE, null, emptyList(), text);
      // can safely ignore the ID returned from storeObj() - it's fine, if the obj already exists
      persist.storeObj(string);
      b.addAdds(commitAdd(KEY_REPO_DESCRIPTION, 0, requireNonNull(string.id()), null, null));
    } catch (ObjTooLargeException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"JavaTimeDefaultTimeZone"})
  private void initializeInternalRef(
      InternalRef internalRef, Consumer<CreateCommit.Builder> commitEnhancer) {
    Reference reference = persist.fetchReference(internalRef.name());

    if (reference == null) {
      CreateCommit.Builder c =
          newCommitBuilder()
              .parentCommitId(EMPTY_OBJ_ID)
              .headers(EMPTY_COMMIT_HEADERS)
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
