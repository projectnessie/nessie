/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.tx;

import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToCommitLogEntry;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRepoDescription;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.assignConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.commitConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.createConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.deleteConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.mergeConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceAlreadyExists;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.repoDescUpdateConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.transplantConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.verifyExpectedHash;
import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;
import static org.projectnessie.versioned.persist.adapter.spi.TryLoopState.newTryLoopState;
import static org.projectnessie.versioned.persist.tx.TxDatabaseAdapter.OpResult.opResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.MustBeClosed;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.InvalidProtocolBufferException;
import org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommitResult;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.ResultType;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.CommitParams;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.persist.adapter.events.AdapterEvent;
import org.projectnessie.versioned.persist.adapter.events.AdapterEventConsumer;
import org.projectnessie.versioned.persist.adapter.events.CommitEvent;
import org.projectnessie.versioned.persist.adapter.events.MergeEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceAssignedEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceCreatedEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceDeletedEvent;
import org.projectnessie.versioned.persist.adapter.events.RepositoryErasedEvent;
import org.projectnessie.versioned.persist.adapter.events.RepositoryInitializedEvent;
import org.projectnessie.versioned.persist.adapter.events.TransplantEvent;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.Traced;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry.Operation;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogParents;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefType;

/**
 * Transactional/relational {@link AbstractDatabaseAdapter} implementation using JDBC primitives.
 *
 * <p>Concrete implementations must at least provide the concrete column types and, if necessary,
 * provide the implementation to check for constraint-violations.
 */
public abstract class TxDatabaseAdapter
    extends AbstractDatabaseAdapter<ConnectionWrapper, TxDatabaseAdapterConfig> {

  /** Value for {@link SqlStatements#TABLE_NAMED_REFERENCES}.{@code ref_type} for a branch. */
  protected static final String REF_TYPE_BRANCH = "b";

  /** Value for {@link SqlStatements#TABLE_NAMED_REFERENCES}.{@code ref_type} for a tag. */
  protected static final String REF_TYPE_TAG = "t";

  private final TxConnectionProvider<?> db;

  public TxDatabaseAdapter(
      TxDatabaseAdapterConfig config,
      TxConnectionProvider<?> db,
      AdapterEventConsumer eventConsumer) {
    super(config, eventConsumer);

    // get the externally configured TxConnectionProvider
    Objects.requireNonNull(
        db,
        "TxDatabaseAdapter requires a non-null TxConnectionProvider via TxDatabaseAdapterConfig.getConnectionProvider()");

    this.db = db;

    db.setupDatabase(this);
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    try (ConnectionWrapper conn = borrowConnection()) {
      return hashOnRef(conn, namedReference, hashOnReference);
    }
  }

  @Override
  public Map<ContentKey, ContentAndState> values(
      Hash commit, Collection<ContentKey> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    try (ConnectionWrapper conn = borrowConnection()) {
      return fetchValues(conn, commit, keys, keyFilter);
    }
  }

  @Override
  @MustBeClosed
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    return withConnectionWrapper(conn -> readCommitLogStream(conn, offset));
  }

  @Override
  public ReferenceInfo<ByteString> namedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null");

    try (ConnectionWrapper conn = borrowConnection()) {
      ReferenceInfo<ByteString> refInfo = fetchNamedRef(conn, ref);
      Hash defaultBranchHead = namedRefsDefaultBranchHead(conn, params);

      Stream<ReferenceInfo<ByteString>> refs = Stream.of(refInfo);

      try (Stream<ReferenceInfo<ByteString>> namedRefs =
          namedRefsFilterAndEnhance(conn, params, defaultBranchHead, refs)) {
        return namedRefs.findFirst().orElseThrow(() -> referenceNotFound(ref));
      }
    }
  }

  @Override
  @MustBeClosed
  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null.");
    Preconditions.checkArgument(
        namedRefsAnyRetrieves(params), "Must retrieve branches or tags or both.");

    return withConnectionWrapper(
        conn -> {
          Hash defaultBranchHead = namedRefsDefaultBranchHead(conn, params);

          @SuppressWarnings("MustBeClosedChecker")
          Stream<ReferenceInfo<ByteString>> refs = fetchNamedRefs(conn);

          return namedRefsFilterAndEnhance(conn, params, defaultBranchHead, refs);
        });
  }

  @Override
  @MustBeClosed
  public Stream<KeyListEntry> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return withConnectionWrapper(conn -> keysForCommitEntry(conn, commit, keyFilter));
  }

  @Override
  public MergeResult<CommitLogEntry> merge(MergeParams mergeParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    // The spec for 'VersionStore.merge' mentions "(...) until we arrive at a common ancestor",
    // but old implementations allowed a merge even if the "merge-from" and "merge-to" have no
    // common ancestor and did merge "everything" from the "merge-from" into "merge-to".
    //
    // This implementation requires a common-ancestor, where "beginning-of-time" is not a valid
    // common-ancestor.
    //
    // Note: "beginning-of-time" (aka creating a branch without specifying a "create-from")
    // creates a new commit-tree that is decoupled from other commit-trees.
    try {
      AtomicReference<ImmutableMergeResult.Builder<CommitLogEntry>> mergeResultHolder =
          new AtomicReference<>();

      Hash result =
          opLoop(
              "merge",
              mergeParams.getToBranch(),
              false,
              (conn, currentHead) -> {
                long timeInMicros = config.currentTimeInMicros();

                ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
                    MergeResult.<CommitLogEntry>builder()
                        .resultType(ResultType.MERGE)
                        .sourceBranch(mergeParams.getFromBranch());
                mergeResultHolder.set(mergeResult);

                List<CommitLogEntry> writtenCommits = new ArrayList<>();
                Hash toHead =
                    mergeAttempt(
                        conn,
                        timeInMicros,
                        currentHead,
                        h -> {},
                        h -> {},
                        writtenCommits::add,
                        mergeResult::addAddedCommits,
                        mergeParams,
                        mergeResult);

                if (toHead.equals(currentHead)) {
                  // nothing done
                  return opResult(currentHead, null);
                }

                Hash resultHash =
                    tryMoveNamedReference(conn, mergeParams.getToBranch(), currentHead, toHead);

                return opResult(
                    resultHash,
                    () ->
                        MergeEvent.builder()
                            .previousHash(currentHead)
                            .hash(resultHash)
                            .branch(mergeParams.getToBranch())
                            .commits(writtenCommits));
              },
              () -> mergeConflictMessage("Conflict", mergeParams),
              () -> mergeConflictMessage("Retry-failure", mergeParams));

      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
          Objects.requireNonNull(
              mergeResultHolder.get(), "Internal error, merge-result builder not set.");
      if (!mergeParams.isDryRun()) {
        mergeResult.wasApplied(true);
      }
      return mergeResult.resultantTargetHash(result).build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public MergeResult<CommitLogEntry> transplant(TransplantParams transplantParams)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      AtomicReference<ImmutableMergeResult.Builder<CommitLogEntry>> mergeResultHolder =
          new AtomicReference<>();

      Hash result =
          opLoop(
              "transplant",
              transplantParams.getToBranch(),
              false,
              (conn, currentHead) -> {
                long timeInMicros = config.currentTimeInMicros();

                ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
                    MergeResult.<CommitLogEntry>builder()
                        .resultType(ResultType.TRANSPLANT)
                        .sourceBranch(transplantParams.getFromBranch());

                mergeResultHolder.set(mergeResult);

                List<CommitLogEntry> writtenCommits = new ArrayList<>();
                Hash targetHead =
                    transplantAttempt(
                        conn,
                        timeInMicros,
                        currentHead,
                        h -> {},
                        h -> {},
                        writtenCommits::add,
                        mergeResult::addAddedCommits,
                        transplantParams,
                        mergeResult);

                Hash resultHash =
                    tryMoveNamedReference(
                        conn, transplantParams.getToBranch(), currentHead, targetHead);

                return opResult(
                    resultHash,
                    () ->
                        TransplantEvent.builder()
                            .previousHash(currentHead)
                            .hash(resultHash)
                            .branch(transplantParams.getToBranch())
                            .commits(writtenCommits));
              },
              () -> transplantConflictMessage("Conflict", transplantParams),
              () -> transplantConflictMessage("Retry-failure", transplantParams));

      ImmutableMergeResult.Builder<CommitLogEntry> mergeResult =
          Objects.requireNonNull(
              mergeResultHolder.get(), "Internal error, merge-result builder not set.");
      if (!transplantParams.isDryRun()) {
        mergeResult.wasApplied(true);
      }
      return mergeResult.resultantTargetHash(result).build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CommitResult<CommitLogEntry> commit(CommitParams commitParams)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      AtomicReference<ImmutableCommitResult.Builder<CommitLogEntry>> commitResultHolder =
          new AtomicReference<>();

      opLoop(
          "commit",
          commitParams.getToBranch(),
          false,
          (conn, branchHead) -> {
            long timeInMicros = config.currentTimeInMicros();

            ImmutableCommitResult.Builder<CommitLogEntry> commitResult = CommitResult.builder();

            CommitLogEntry newBranchCommit =
                commitAttempt(conn, timeInMicros, branchHead, commitParams, h -> {});

            Hash resultHash =
                tryMoveNamedReference(
                    conn, commitParams.getToBranch(), branchHead, newBranchCommit.getHash());

            commitResult.commit(newBranchCommit).targetBranch(commitParams.getToBranch());
            commitResultHolder.set(commitResult);

            return opResult(
                resultHash,
                () ->
                    CommitEvent.builder()
                        .previousHash(branchHead)
                        .hash(resultHash)
                        .branch(commitParams.getToBranch())
                        .addCommits(newBranchCommit));
          },
          () ->
              commitConflictMessage(
                  "Conflict", commitParams.getToBranch(), commitParams.getExpectedHead()),
          () ->
              commitConflictMessage(
                  "Retry-Failure", commitParams.getToBranch(), commitParams.getExpectedHead()));

      ImmutableCommitResult.Builder<CommitLogEntry> commitResult =
          Objects.requireNonNull(
              commitResultHolder.get(), "Internal error, commit-result builder not set.");

      return commitResult.build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public ReferenceCreatedResult create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    try {
      AtomicReference<ImmutableReferenceCreatedResult.Builder> resultHolder =
          new AtomicReference<>();

      opLoop(
          "createRef",
          ref,
          true,
          (conn, nullHead) -> {
            ImmutableReferenceCreatedResult.Builder result =
                ImmutableReferenceCreatedResult.builder().namedRef(ref);
            resultHolder.set(result);

            if (checkNamedRefExistence(conn, ref.getName())) {
              throw referenceAlreadyExists(ref);
            }

            Hash hash =
                target != null
                    ? target
                    // Special case: Don't validate, if the 'target' parameter is null.
                    // This is mostly used for tests that re-create the default-branch.
                    : NO_ANCESTOR;

            validateHashExists(conn, hash);

            insertNewReference(conn, ref, hash);

            commitRefLog(
                conn,
                config.currentTimeInMicros(),
                hash,
                ref,
                RefLogEntry.Operation.CREATE_REFERENCE,
                emptyList());

            result.hash(hash);

            return opResult(hash, () -> ReferenceCreatedEvent.builder().currentHash(hash).ref(ref));
          },
          () -> createConflictMessage("Conflict", ref, target),
          () -> createConflictMessage("Retry-Failure", ref, target));

      ImmutableReferenceCreatedResult.Builder result =
          Objects.requireNonNull(
              resultHolder.get(), "Internal error, reference-result builder not set.");

      return result.build();
    } catch (ReferenceAlreadyExistsException | ReferenceNotFoundException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReferenceDeletedResult delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      AtomicReference<ImmutableReferenceDeletedResult.Builder> resultHolder =
          new AtomicReference<>();

      opLoop(
          "deleteRef",
          reference,
          false,
          (conn, pointer) -> {
            ImmutableReferenceDeletedResult.Builder result =
                ImmutableReferenceDeletedResult.builder().namedRef(reference);
            resultHolder.set(result);

            verifyExpectedHash(pointer, reference, expectedHead);

            Hash commitHash = fetchNamedRefHead(conn, reference);
            try (Traced ignore = trace("deleteRefInDb");
                PreparedStatement ps =
                    conn.conn().prepareStatement(SqlStatements.DELETE_NAMED_REFERENCE)) {
              ps.setString(1, config.getRepositoryId());
              ps.setString(2, reference.getName());
              ps.setString(3, pointer.asString());
              if (ps.executeUpdate() != 1) {
                return null;
              }
            }

            commitRefLog(
                conn,
                config.currentTimeInMicros(),
                commitHash,
                reference,
                RefLogEntry.Operation.DELETE_REFERENCE,
                emptyList());

            result.hash(commitHash);

            return opResult(
                pointer,
                () -> ReferenceDeletedEvent.builder().currentHash(commitHash).ref(reference));
          },
          () -> deleteConflictMessage("Conflict", reference, expectedHead),
          () -> deleteConflictMessage("Retry-Failure", reference, expectedHead));

      ImmutableReferenceDeletedResult.Builder result =
          Objects.requireNonNull(
              resultHolder.get(), "Internal error, reference-result builder not set.");

      return result.build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReferenceAssignedResult assign(
      NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      AtomicReference<ImmutableReferenceAssignedResult.Builder> resultHolder =
          new AtomicReference<>();

      opLoop(
          "assignRef",
          assignee,
          false,
          (conn, assigneeHead) -> {
            ImmutableReferenceAssignedResult.Builder result =
                ImmutableReferenceAssignedResult.builder().namedRef(assignee);
            resultHolder.set(result);

            verifyExpectedHash(assigneeHead, assignee, expectedHead);

            validateHashExists(conn, assignTo);

            Hash resultHash = tryMoveNamedReference(conn, assignee, assigneeHead, assignTo);

            commitRefLog(
                conn,
                config.currentTimeInMicros(),
                assignTo,
                assignee,
                RefLogEntry.Operation.ASSIGN_REFERENCE,
                Collections.singletonList(assigneeHead));

            result.previousHash(assigneeHead).currentHash(assignTo);

            return opResult(
                resultHash,
                () ->
                    ReferenceAssignedEvent.builder()
                        .currentHash(assignTo)
                        .ref(assignee)
                        .previousHash(assigneeHead));
          },
          () -> assignConflictMessage("Conflict", assignee, expectedHead, assignTo),
          () -> assignConflictMessage("Retry-Failure", assignee, expectedHead, assignTo));

      ImmutableReferenceAssignedResult.Builder result =
          Objects.requireNonNull(
              resultHolder.get(), "Internal error, reference-result builder not set.");

      return result.build();
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @MustBeClosed
  public Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    return withConnectionWrapper(conn -> buildDiff(conn, from, to, keyFilter));
  }

  @Override
  public void initializeRepo(String defaultBranchName) {
    try (ConnectionWrapper conn = borrowConnection()) {
      BranchName defaultBranch = BranchName.of(defaultBranchName);
      if (!checkNamedRefExistence(conn, defaultBranch)) {
        // note: no need to initialize the repo-description

        insertNewReference(conn, defaultBranch, NO_ANCESTOR);

        RefLogEntry newRefLog =
            writeRefLogEntry(
                conn,
                defaultBranch,
                RefLogHead.builder()
                    .refLogHead(NO_ANCESTOR)
                    .addRefLogParentsInclHead(NO_ANCESTOR)
                    .build(),
                NO_ANCESTOR,
                RefLogEntry.Operation.CREATE_REFERENCE,
                config.currentTimeInMicros(),
                emptyList());
        insertRefLogHead(newRefLog, conn);

        conn.commit();

        repositoryEvent(
            () -> RepositoryInitializedEvent.builder().defaultBranch(defaultBranchName));
        repositoryEvent(
            () -> ReferenceCreatedEvent.builder().ref(defaultBranch).currentHash(NO_ANCESTOR));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void eraseRepo() {
    try (ConnectionWrapper conn = borrowConnection()) {
      try (PreparedStatement ps =
          conn.conn().prepareStatement(SqlStatements.DELETE_NAMED_REFERENCE_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.conn().prepareStatement(SqlStatements.DELETE_GLOBAL_STATE_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.conn().prepareStatement(SqlStatements.DELETE_COMMIT_LOG_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.conn().prepareStatement(SqlStatements.DELETE_KEY_LIST_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.conn().prepareStatement(SqlStatements.DELETE_REF_LOG_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.conn().prepareStatement(SqlStatements.DELETE_REF_LOG_HEAD_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.conn().prepareStatement(SqlStatements.DELETE_REPO_DESCRIPTIONE_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }

      conn.commit();

      repositoryEvent(RepositoryErasedEvent::builder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<ContentIdAndBytes> globalContent(ContentId contentId) {
    try (ConnectionWrapper conn = borrowConnection()) {
      try (Traced ignore = trace("globalContent");
          PreparedStatement ps =
              conn.conn()
                  .prepareStatement(String.format(SqlStatements.SELECT_GLOBAL_STATE_MANY, "?"))) {
        ps.setString(1, config.getRepositoryId());
        ps.setString(2, contentId.getId());
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return Optional.of(globalContentFromRow(rs));
          }
        }
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @MustBeClosed
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    return withConnectionWrapper(conn -> readRefLogStream(conn, offset));
  }

  @Override
  public Map<String, Map<String, String>> repoMaintenance(
      RepoMaintenanceParams repoMaintenanceParams) {
    // Nothing to do
    return Collections.emptyMap();
  }

  @Override
  public void assertCleanStateForTests() {
    if (ConnectionWrapper.threadHasOpenConnection()) {
      try {
        throw new IllegalStateException("Current thread has unclosed database connection");
      } finally {
        ConnectionWrapper.borrow(() -> null).forceClose();
      }
    }
  }

  private static ContentIdAndBytes globalContentFromRow(ResultSet rs) throws SQLException {
    ContentId cid = ContentId.of(rs.getString(1));
    ByteString value = UnsafeByteOperations.unsafeWrap(rs.getBytes(2));
    return ContentIdAndBytes.of(cid, value);
  }

  @Override
  public void writeMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceConflictException {
    try (ConnectionWrapper conn = borrowConnection()) {
      doWriteMultipleCommits(conn, commitLogEntries);
      conn.commit();
    } catch (ReferenceConflictException e) {
      throw e;
    }
  }

  @Override
  public void updateMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceNotFoundException {
    try (ConnectionWrapper conn = borrowConnection()) {
      doUpdateMultipleCommits(conn, commitLogEntries);
      conn.commit();
    } catch (ReferenceNotFoundException e) {
      throw e;
    }
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // Transactional DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Convenience for {@link AbstractDatabaseAdapter#hashOnRef(AutoCloseable, NamedRef, Optional,
   * Hash) hashOnRef(conn, ref, fetchNamedRefHead(conn, ref.getReference()))}.
   */
  protected Hash hashOnRef(ConnectionWrapper conn, NamedRef reference, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(conn, reference, hashOnRef, fetchNamedRefHead(conn, reference));
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyListEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  public ConnectionWrapper borrowConnection() {
    return ConnectionWrapper.borrow(this::newConnection);
  }

  protected Connection newConnection() {
    try {
      return db.borrowConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  public interface LoopOp {
    /**
     * Applies an operation within a CAS-loop. The implementation gets the current global-state and
     * must return an updated global-state with a different global-id.
     *
     * @param conn current JDBC connection
     * @param targetRefHead current named-reference's HEAD
     * @return if non-{@code * null}, the JDBC transaction is committed and the hash returned from
     *     {@link #opLoop(String, NamedRef, boolean, LoopOp, Supplier, Supplier)}. If {@code null},
     *     the {@code opLoop()} tries again.
     * @throws RetryTransactionException (see {@link #isRetryTransaction(SQLException)}), the
     *     current JDBC transaction is rolled back and {@link #opLoop(String, NamedRef, boolean,
     *     LoopOp, Supplier, Supplier)} tries again
     * @throws SQLException if this matches {@link #isIntegrityConstraintViolation(Throwable)}, the
     *     exception is re-thrown as a {@link ReferenceConflictException}
     * @throws VersionStoreException any other version-store exception
     * @see #opLoop(String, NamedRef, boolean, LoopOp, Supplier, Supplier)
     */
    OpResult apply(ConnectionWrapper conn, Hash targetRefHead)
        throws VersionStoreException, SQLException;
  }

  static final class OpResult {
    final Hash head;
    final Supplier<? extends AdapterEvent.Builder<?, ?>> adapterEventBuilder;

    private OpResult(
        Hash head, Supplier<? extends AdapterEvent.Builder<?, ?>> adapterEventBuilder) {
      this.head = head;
      this.adapterEventBuilder = adapterEventBuilder;
    }

    public static OpResult opResult(
        Hash head, Supplier<? extends AdapterEvent.Builder<?, ?>> adapterEventBuilder) {
      return new OpResult(head, adapterEventBuilder);
    }
  }

  /**
   * This is the actual CAS-ish-loop, which applies an operation onto a named-ref.
   *
   * <p>Each CAS-loop-iteration fetches the current HEAD of the named reference and calls {@link
   * LoopOp#apply(ConnectionWrapper, Hash)}. If {@code apply()} throws a {@link
   * RetryTransactionException} (see {@link #isRetryTransaction(SQLException)}), the current JDBC
   * transaction is rolled back and the next loop-iteration starts, unless the retry-policy allows
   * no more retries, in which case a {@link ReferenceRetryFailureException} with the message from
   * {@code retryErrorMessage} is thrown. If the thrown exception is a {@link
   * #isIntegrityConstraintViolation(Throwable)}, the exception is re-thrown as a {@link
   * ReferenceConflictException} with an appropriate message from the {@code conflictErrorMessage}
   * supplier.
   *
   * <p>If {@link LoopOp#apply(ConnectionWrapper, Hash)} completes normally and returns a non-{@code
   * null} hash, the JDBC transaction is committed and the hash returned from this function. If
   * {@code apply()} returns {@code null}, the operation is retried, unless the retry-policy allows
   * no more retries, in which * case a {@link ReferenceRetryFailureException} with the message from
   * {@code retryErrorMessage} * is thrown.
   *
   * <p>Uses {@link TryLoopState} for retry handling.
   *
   * @param namedReference the named reference on which the Nessie operation works
   * @param createRef flag, whether this ia a "create-named-reference" operation, which skips the
   *     retrieval of the current HEAD
   * @param loopOp the implementation of the Nessie operation
   * @param conflictErrorMessage message producer to represent an unresolvable conflict in the data
   * @param retryErrorMessage message producer to represent that no more retries will happen
   * @see LoopOp#apply(ConnectionWrapper, Hash)
   */
  protected Hash opLoop(
      String opName,
      NamedRef namedReference,
      boolean createRef,
      LoopOp loopOp,
      Supplier<String> conflictErrorMessage,
      Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    try (ConnectionWrapper conn = borrowConnection();
        TryLoopState tryState =
            newTryLoopState(
                opName,
                ts ->
                    String.format(
                        "%s after %d retries, %d ms",
                        retryErrorMessage.get(),
                        ts.getRetries(),
                        ts.getDuration(TimeUnit.MILLISECONDS)),
                this::tryLoopStateCompletion,
                config)) {
      while (true) {
        Hash pointer = createRef ? null : fetchNamedRefHead(conn, namedReference);

        try {
          OpResult opResult = loopOp.apply(conn, pointer);

          repositoryEvent(opResult.adapterEventBuilder);

          // The operation succeeded, if it returns a non-null hash value.
          if (opResult.head != null) {
            conn.commit();
            return tryState.success(opResult.head);
          }
        } catch (RetryTransactionException e) {
          conn.rollback();
          tryState.retry();
          continue;
        } catch (SQLException e) {
          if (isRetryTransaction(e)) {
            conn.rollback();
            tryState.retry();
            continue;
          }
          throwIfReferenceConflictException(e, conflictErrorMessage);
          throw new RuntimeException(e);
        }

        conn.rollback();
        tryState.retry();
      }
    }
  }

  @MustBeClosed
  protected Stream<ReferenceInfo<ByteString>> fetchNamedRefs(ConnectionWrapper conn) {
    return JdbcSelectSpliterator.buildStream(
        conn.conn(),
        SqlStatements.SELECT_NAMED_REFERENCES,
        ps -> ps.setString(1, config.getRepositoryId()),
        (rs) -> {
          String type = rs.getString(1);
          String ref = rs.getString(2);
          Hash head = Hash.of(rs.getString(3));

          NamedRef namedRef = namedRefFromRow(type, ref);
          if (namedRef != null) {
            return ReferenceInfo.of(head, namedRef);
          }
          return null;
        });
  }

  /**
   * Similar to {@link #fetchNamedRefHead(ConnectionWrapper, NamedRef)}, but just checks for
   * existence.
   */
  protected boolean checkNamedRefExistence(ConnectionWrapper c, NamedRef ref) {
    try {
      fetchNamedRefHead(c, ref);
      return true;
    } catch (ReferenceNotFoundException e) {
      return false;
    }
  }

  /**
   * Similar to {@link #fetchNamedRefHead(ConnectionWrapper, NamedRef)}, but just checks for
   * existence.
   */
  protected boolean checkNamedRefExistence(ConnectionWrapper c, String refName) {
    try (Traced ignore = trace("checkNamedRefExistence");
        PreparedStatement ps =
            c.conn().prepareStatement(SqlStatements.SELECT_NAMED_REFERENCE_NAME)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, refName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieves the current HEAD for a reference, throws a {@link ReferenceNotFoundException}, it the
   * reference does not exist.
   */
  protected Hash fetchNamedRefHead(ConnectionWrapper c, NamedRef ref)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("fetchNamedRefHead");
        PreparedStatement ps = c.conn().prepareStatement(SqlStatements.SELECT_NAMED_REFERENCE)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, ref.getName());
      ps.setString(3, referenceTypeDiscriminator(ref));
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Hash.of(rs.getString(1));
        }
        throw referenceNotFound(ref);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected ReferenceInfo<ByteString> fetchNamedRef(ConnectionWrapper c, String ref)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("fetchNamedRef");
        PreparedStatement ps =
            c.conn().prepareStatement(SqlStatements.SELECT_NAMED_REFERENCE_ANY)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, ref);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          Hash hash = Hash.of(rs.getString(2));
          NamedRef namedRef = namedRefFromRow(rs.getString(1), ref);
          return ReferenceInfo.of(hash, namedRef);
        }
        throw referenceNotFound(ref);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected static NamedRef namedRefFromRow(String type, String ref) {
    switch (type) {
      case REF_TYPE_BRANCH:
        return BranchName.of(ref);
      case REF_TYPE_TAG:
        return TagName.of(ref);
      default:
        return null;
    }
  }

  protected void insertNewReference(ConnectionWrapper conn, NamedRef ref, Hash hash)
      throws ReferenceAlreadyExistsException, SQLException {
    try (Traced ignore = trace("insertNewReference");
        PreparedStatement ps = conn.conn().prepareStatement(SqlStatements.INSERT_NAMED_REFERENCE)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, ref.getName());
      ps.setString(3, referenceTypeDiscriminator(ref));
      ps.setString(4, hash.asString());
      ps.executeUpdate();
    } catch (SQLException e) {
      if (isIntegrityConstraintViolation(e)) {
        throw referenceAlreadyExists(ref);
      }
      throw e;
    }
  }

  protected static String referenceTypeDiscriminator(NamedRef ref) {
    String refType;
    if (ref instanceof BranchName) {
      refType = REF_TYPE_BRANCH;
    } else if (ref instanceof TagName) {
      refType = REF_TYPE_TAG;
    } else {
      throw new IllegalArgumentException();
    }
    return refType;
  }

  /**
   * Retrieves the hash of the default branch specified in {@link
   * GetNamedRefsParams#getBaseReference()}, if the retrieve options in {@link GetNamedRefsParams}
   * require it.
   */
  private Hash namedRefsDefaultBranchHead(ConnectionWrapper conn, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    if (namedRefsRequiresBaseReference(params)) {
      Preconditions.checkNotNull(params.getBaseReference(), "Base reference name missing.");
      return fetchNamedRefHead(conn, params.getBaseReference());
    }
    return null;
  }

  protected String sqlForManyPlaceholders(String sql, int num) {
    String placeholders =
        IntStream.range(0, num).mapToObj(x -> "?").collect(Collectors.joining(", "));
    return String.format(sql, placeholders);
  }

  @Override
  protected Map<ContentId, ByteString> doFetchGlobalStates(
      ConnectionWrapper conn, Set<ContentId> contentIds) {
    Map<ContentId, ByteString> result = new HashMap<>();
    if (contentIds.isEmpty()) {
      return result;
    }

    String sql = sqlForManyPlaceholders(SqlStatements.SELECT_GLOBAL_STATE_MANY, contentIds.size());

    try (PreparedStatement ps = conn.conn().prepareStatement(sql)) {
      ps.setString(1, config.getRepositoryId());
      int i = 2;
      for (ContentId cid : contentIds) {
        ps.setString(i++, cid.getId());
      }

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ContentId contentId = ContentId.of(rs.getString(1));
          if (contentIds.contains(contentId)) {
            byte[] data = rs.getBytes(2);
            ByteString val = UnsafeByteOperations.unsafeWrap(data);
            result.put(contentId, val);
          }
        }
      }
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Stream<CommitLogEntry> doScanAllCommitLogEntries(ConnectionWrapper c) {
    return JdbcSelectSpliterator.buildStream(
        c.conn(),
        SqlStatements.SELECT_COMMIT_LOG_FULL,
        ps -> ps.setString(1, config.getRepositoryId()),
        (rs) -> protoToCommitLogEntry(rs.getBytes(1)));
  }

  @Override
  protected CommitLogEntry doFetchFromCommitLog(ConnectionWrapper c, Hash hash) {
    try (PreparedStatement ps = c.conn().prepareStatement(SqlStatements.SELECT_COMMIT_LOG)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, hash.asString());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? protoToCommitLogEntry(rs.getBytes(1)) : null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<CommitLogEntry> doFetchMultipleFromCommitLog(
      ConnectionWrapper c, List<Hash> hashes) {
    if (hashes.isEmpty()) {
      return emptyList();
    }

    String sql = sqlForManyPlaceholders(SqlStatements.SELECT_COMMIT_LOG_MANY, hashes.size());

    try (PreparedStatement ps = c.conn().prepareStatement(sql)) {
      ps.setString(1, config.getRepositoryId());
      for (int i = 0; i < hashes.size(); i++) {
        ps.setString(2 + i, hashes.get(i).asString());
      }

      Map<Hash, CommitLogEntry> result = new HashMap<>(hashes.size() * 2);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          CommitLogEntry entry = protoToCommitLogEntry(rs.getBytes(1));
          result.put(entry.getHash(), entry);
        }
      }
      return hashes.stream().map(result::get).collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doWriteIndividualCommit(ConnectionWrapper c, CommitLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = c.conn().prepareStatement(SqlStatements.INSERT_COMMIT_LOG)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, entry.getHash().asString());
      ps.setBytes(3, toProto(entry).toByteArray());
      ps.executeUpdate();
    } catch (SQLException e) {
      if (isRetryTransaction(e)) {
        throw new RetryTransactionException();
      }
      throwIfReferenceConflictException(
          e, () -> String.format("Hash collision for '%s' in commit-log", entry.getHash()));
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doWriteMultipleCommits(ConnectionWrapper c, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    try {
      writeMany(
          c,
          SqlStatements.INSERT_COMMIT_LOG,
          entries,
          e -> e.getHash().asString(),
          e -> toProto(e).toByteArray(),
          false);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doUpdateMultipleCommits(ConnectionWrapper c, List<CommitLogEntry> entries)
      throws ReferenceNotFoundException {
    try {
      writeMany(
          c,
          SqlStatements.UPDATE_COMMIT_LOG,
          entries,
          e -> e.getHash().asString(),
          e -> toProto(e).toByteArray(),
          true);
    } catch (ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doWriteKeyListEntities(
      ConnectionWrapper c, List<KeyListEntity> newKeyListEntities) {
    try {
      writeMany(
          c,
          SqlStatements.INSERT_KEY_LIST,
          newKeyListEntities,
          e -> e.getId().asString(),
          e -> toProto(e.getKeys()).toByteArray(),
          false);
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  protected <T> void writeMany(
      ConnectionWrapper c,
      String sqlInsert,
      List<T> entries,
      Function<T, String> idRetriever,
      Function<T, byte[]> serializer,
      boolean update)
      throws ReferenceConflictException, ReferenceNotFoundException {
    int cnt = 0;
    try (PreparedStatement ps = c.conn().prepareStatement(sqlInsert)) {
      for (T e : entries) {
        if (update) {
          ps.setBytes(1, serializer.apply(e));
          ps.setString(2, config.getRepositoryId());
          ps.setString(3, idRetriever.apply(e));
        } else {
          ps.setString(1, config.getRepositoryId());
          ps.setString(2, idRetriever.apply(e));
          ps.setBytes(3, serializer.apply(e));
        }
        ps.addBatch();
        cnt++;
        if (cnt == config.getBatchSize()) {
          int[] result = ps.executeBatch();
          if (update) {
            for (int i : result) {
              if (i != 1) {
                throw new ReferenceNotFoundException("");
              }
            }
          }
          cnt = 0;
        }
      }
      if (cnt > 0) {
        int[] result = ps.executeBatch();
        if (update) {
          for (int i : result) {
            if (i != 1) {
              throw new ReferenceNotFoundException("");
            }
          }
        }
      }
    } catch (SQLException e) {
      if (isRetryTransaction(e)) {
        throw new RetryTransactionException();
      }
      throwIfReferenceConflictException(
          e,
          () ->
              String.format(
                  "Hash collision for one of the hashes %s in commit-log",
                  entries.stream()
                      .map(x -> "'" + idRetriever.apply(x) + "'")
                      .collect(Collectors.joining(", "))));
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Stream<KeyListEntity> doFetchKeyLists(ConnectionWrapper c, List<Hash> keyListsIds) {
    if (keyListsIds.isEmpty()) {
      return Stream.empty();
    }

    try (Traced ignore = trace("doFetchKeyLists.stream")) {
      return JdbcSelectSpliterator.buildStream(
          c.conn(),
          sqlForManyPlaceholders(SqlStatements.SELECT_KEY_LIST_MANY, keyListsIds.size()),
          ps -> {
            ps.setString(1, config.getRepositoryId());
            int i = 2;
            for (Hash id : keyListsIds) {
              ps.setString(i++, id.asString());
            }
          },
          (rs) -> KeyListEntity.of(Hash.of(rs.getString(1)), protoToKeyList(rs.getBytes(2))));
    }
  }

  /**
   * If {@code e} represents an {@link #isIntegrityConstraintViolation(Throwable)
   * integrity-constraint-violation}, throw a {@link ReferenceConflictException} using the message
   * produced by {@code message}.
   */
  protected void throwIfReferenceConflictException(SQLException e, Supplier<String> message)
      throws ReferenceConflictException {
    if (isIntegrityConstraintViolation(e)) {
      throw new ReferenceConflictException(message.get(), e);
    }
  }

  /** Deadlock error, returned by Postgres. */
  protected static final String DEADLOCK_SQL_STATE_POSTGRES = "40P01";

  /**
   * Cockroach "retry, write too old" * error, see <a
   * href="https://www.cockroachlabs.com/docs/v21.1/transaction-retry-error-reference.html#retry_write_too_old">Cockroach's
   * Transaction Retry Error Reference</a>, and Postgres may return a "deadlock" error.
   */
  protected static final String RETRY_SQL_STATE_COCKROACH = "40001";

  /** Postgres &amp; Cockroach integrity constraint violation. */
  protected static final String CONSTRAINT_VIOLATION_SQL_STATE = "23505";

  /** H2 integrity constraint violation. */
  protected static final int CONSTRAINT_VIOLATION_SQL_CODE = 23505;

  /** Returns an exception that indicates an integrity-constraint-violation. */
  protected SQLException newIntegrityConstraintViolationException() {
    return new SQLIntegrityConstraintViolationException();
  }

  /**
   * Check whether the given {@link Throwable} represents an exception that indicates an
   * integrity-constraint-violation.
   */
  protected boolean isIntegrityConstraintViolation(Throwable e) {
    if (e instanceof SQLException) {
      SQLException sqlException = (SQLException) e;
      return sqlException instanceof SQLIntegrityConstraintViolationException
          // e.g. H2
          || CONSTRAINT_VIOLATION_SQL_CODE == sqlException.getErrorCode()
          // e.g. Postgres & Cockroach
          || CONSTRAINT_VIOLATION_SQL_STATE.equals(sqlException.getSQLState());
    }
    return false;
  }

  /**
   * Check whether the {@link SQLException} indicates a "retry hint". This can happen when there is
   * too much contention on the database rows. Cockroach may throw return a "retry, write too old"
   * error, see <a
   * href="https://www.cockroachlabs.com/docs/v21.1/transaction-retry-error-reference.html#retry_write_too_old">Cockroach's
   * Transaction Retry Error Reference</a>, and Postgres may return a "deadlock" error.
   */
  protected boolean isRetryTransaction(SQLException e) {
    if (e.getSQLState() == null) {
      return false;
    }
    switch (e.getSQLState()) {
      case DEADLOCK_SQL_STATE_POSTGRES:
      case RETRY_SQL_STATE_COCKROACH:
        return true;
      default:
        return false;
    }
  }

  /**
   * Updates the HEAD of the given {@code ref} from {@code expectedHead} to {@code newHead}. Returns
   * {@code newHead}, if successful, and {@code null} if not.
   */
  protected Hash tryMoveNamedReference(
      ConnectionWrapper conn, NamedRef ref, Hash expectedHead, Hash newHead) {
    try (Traced ignore = trace("tryMoveNamedReference");
        PreparedStatement ps = conn.conn().prepareStatement(SqlStatements.UPDATE_NAMED_REFERENCE)) {
      ps.setString(1, newHead.asString());
      ps.setString(2, config.getRepositoryId());
      ps.setString(3, ref.getName());
      ps.setString(4, expectedHead.asString());
      return ps.executeUpdate() == 1 ? newHead : null;
    } catch (SQLException e) {
      if (isRetryTransaction(e)) {
        throw new RetryTransactionException();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public RepoDescription fetchRepositoryDescription() {
    try (ConnectionWrapper conn = borrowConnection()) {
      return fetchRepositoryDescription(conn);
    }
  }

  private RepoDescription fetchRepositoryDescription(ConnectionWrapper conn) {
    try {
      return protoToRepoDescription(fetchRepositoryDescriptionInternal(conn));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] fetchRepositoryDescriptionInternal(ConnectionWrapper conn) throws SQLException {
    try (Traced ignore = trace("fetchRepositoryDescriptionInternal");
        PreparedStatement ps =
            conn.conn().prepareStatement(SqlStatements.SELECT_REPO_DESCRIPTION)) {
      ps.setString(1, config.getRepositoryId());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getBytes(1);
        }
      }

      return null;
    }
  }

  @Override
  public void updateRepositoryDescription(Function<RepoDescription, RepoDescription> updater)
      throws ReferenceConflictException {

    try {
      opLoop(
          "updateRepositoryDescription",
          null,
          true,
          (conn, x) -> {
            byte[] currentBytes = fetchRepositoryDescriptionInternal(conn);

            RepoDescription current = protoToRepoDescription(currentBytes);
            RepoDescription updated = updater.apply(current);

            if (updated != null) {
              if (currentBytes == null) {
                try (PreparedStatement ps =
                    conn.conn().prepareStatement(SqlStatements.INSERT_REPO_DESCRIPTION)) {
                  ps.setString(1, config.getRepositoryId());
                  ps.setBytes(2, toProto(updated).toByteArray());
                  if (ps.executeUpdate() == 0) {
                    return null;
                  }
                }
              } else {
                try (PreparedStatement ps =
                    conn.conn().prepareStatement(SqlStatements.UPDATE_REPO_DESCRIPTION)) {
                  ps.setBytes(1, toProto(updated).toByteArray());
                  ps.setString(2, config.getRepositoryId());
                  ps.setBytes(3, currentBytes);
                  if (ps.executeUpdate() == 0) {
                    return null;
                  }
                }
              }
            }

            return opResult(NO_ANCESTOR, null);
          },
          () -> repoDescUpdateConflictMessage("Conflict"),
          () -> repoDescUpdateConflictMessage("Retry-failure"));

    } catch (ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Provides a map of table name to create-table-DDL. The DDL statements are processed by {@link
   * java.text.MessageFormat} to inject the parameters returned by {@link
   * #databaseSqlFormatParameters()}, which is for example used to have the "proper", database
   * specific column types.
   *
   * <p>Names of the tables are defined by the constants defined in this class that start with
   * {@code TABLE_}, for example {@link SqlStatements#TABLE_COMMIT_LOG}.
   *
   * <p>The DDL statements in the returned map's value are formatted using {@link
   * java.text.MessageFormat} using the enum map from {@link #databaseSqlFormatParameters()}, where
   * the ordinal of the {@link NessieSqlDataType} enum is used as the index.
   *
   * @see NessieSqlDataType
   * @see #databaseSqlFormatParameters() ()
   */
  protected Map<String, List<String>> allCreateTableDDL() {
    return ImmutableMap.<String, List<String>>builder()
        .put(
            SqlStatements.TABLE_REPO_DESCRIPTION,
            Collections.singletonList(SqlStatements.CREATE_TABLE_REPO_DESCRIPTION))
        .put(
            SqlStatements.TABLE_GLOBAL_STATE,
            Collections.singletonList(SqlStatements.CREATE_TABLE_GLOBAL_STATE))
        .put(
            SqlStatements.TABLE_NAMED_REFERENCES,
            Collections.singletonList(SqlStatements.CREATE_TABLE_NAMED_REFERENCES))
        .put(
            SqlStatements.TABLE_COMMIT_LOG,
            Collections.singletonList(SqlStatements.CREATE_TABLE_COMMIT_LOG))
        .put(
            SqlStatements.TABLE_KEY_LIST,
            Collections.singletonList(SqlStatements.CREATE_TABLE_KEY_LIST))
        .put(
            SqlStatements.TABLE_REF_LOG,
            Collections.singletonList(SqlStatements.CREATE_TABLE_REF_LOG))
        .put(
            SqlStatements.TABLE_REF_LOG_HEAD,
            Collections.singletonList(SqlStatements.CREATE_TABLE_REF_LOG_HEAD))
        .build();
  }

  /**
   * Get database-specific 'strings' like column definitions for 'BLOB' column types. Used as
   * placeholders to format the DDL statements from {@link #allCreateTableDDL()}.
   *
   * @see NessieSqlDataType
   * @see #allCreateTableDDL()
   */
  protected abstract Map<NessieSqlDataType, String> databaseSqlFormatParameters();

  /**
   * Some RDBMS' just return an error when an INSERT/UPDATE/DELETE violates a constraint, like a
   * primary key, but other RDBMS' like PostgreSQL do return an error <em>and</em> mark the whole
   * transaction as failed. This function allows is meant to be implemented for RDBMS' that do mark
   * the transaction as failed to modify an INSERT SQL statement to neither yield an error nor mark
   * the transaction as failed.
   */
  protected String insertOnConflictDoNothing(String insertSql) {
    return insertSql;
  }

  /**
   * Defines the types of Nessie data types used to map to SQL datatypes via {@link
   * #databaseSqlFormatParameters()}. For example, the SQL datatype used to store a {@link #BLOB}
   * might be a {@code BLOB} in a specific database and {@code BYTEA} in another.
   *
   * @see #databaseSqlFormatParameters()
   * @see #allCreateTableDDL()
   */
  protected enum NessieSqlDataType {
    /** Column-type string for a 'BLOB' column. */
    BLOB,
    /** Column-type string for the string representation of a {@link Hash}. */
    HASH,
    /** Column-type string for key-prefix. */
    KEY_PREFIX,
    /** Column-type string for the string representation of a {@link ContentKey}. */
    KEY,
    /** Column-type string for the string representation of a {@link NamedRef}. */
    NAMED_REF,
    /** Column-type string for the named-reference-type (single char). */
    NAMED_REF_TYPE,
    /** Column-type string for the content-id. */
    CONTENT_ID,
    /** Column-type string for an integer. */
    INTEGER
  }

  /** Whether the database/JDBC-driver require schema-metadata-queries require upper-case names. */
  protected boolean metadataUpperCase() {
    return true;
  }

  /** Whether this implementation shall use bates for DDL operations to create tables. */
  protected boolean batchDDL() {
    return false;
  }

  protected void updateRefLogHead(RefLogEntry newRefLog, ConnectionWrapper conn)
      throws SQLException {
    try (Traced ignore = trace("updateRefLogHead");
        PreparedStatement psUpdate =
            conn.conn().prepareStatement(SqlStatements.UPDATE_REF_LOG_HEAD)) {
      psUpdate.setString(1, Hash.of(newRefLog.getRefLogId()).asString());
      psUpdate.setBytes(2, refLogHeadParents(newRefLog).toByteArray());
      psUpdate.setString(3, config.getRepositoryId());
      psUpdate.setString(4, Hash.of(newRefLog.getParents(0)).asString());
      if (psUpdate.executeUpdate() != 1) {
        // retry the transaction with rebasing the parent id.
        throw new RetryTransactionException();
      }
    }
  }

  protected void insertRefLogHead(RefLogEntry newRefLog, ConnectionWrapper conn)
      throws SQLException {
    try (Traced ignore = trace("insertRefLogHead");
        PreparedStatement selectStatement =
            conn.conn().prepareStatement(SqlStatements.SELECT_REF_LOG_HEAD)) {
      selectStatement.setString(1, config.getRepositoryId());
      // insert if the table is empty
      try (ResultSet result = selectStatement.executeQuery()) {
        if (!result.next()) {
          try (PreparedStatement psUpdate =
              conn.conn().prepareStatement(SqlStatements.INSERT_REF_LOG_HEAD)) {
            psUpdate.setString(1, config.getRepositoryId());
            psUpdate.setString(2, Hash.of(newRefLog.getRefLogId()).asString());
            psUpdate.setBytes(3, refLogHeadParents(newRefLog).toByteArray());
            if (psUpdate.executeUpdate() != 1) {
              // No need to continue, just throw a legit constraint-violation that will be
              // converted to a "proper ReferenceConflictException" later up in the stack.
              throw newIntegrityConstraintViolationException();
            }
          }
        }
      }
    }
  }

  private RefLogParents refLogHeadParents(RefLogEntry newRefLog) {
    RefLogParents.Builder refLogParents = RefLogParents.newBuilder();
    refLogParents.addRefLogParentsInclHead(newRefLog.getRefLogId());
    newRefLog.getParentsList().stream()
        .limit(config.getParentsPerRefLogEntry())
        .forEach(refLogParents::addRefLogParentsInclHead);
    return refLogParents.build();
  }

  @Override
  protected Spliterator<RefLog> readRefLog(ConnectionWrapper ctx, Hash initialHash)
      throws RefLogNotFoundException {
    if (NO_ANCESTOR.equals(initialHash)) {
      return Spliterators.emptySpliterator();
    }

    RefLog initial = fetchFromRefLog(ctx, initialHash);
    if (initial == null) {
      throw RefLogNotFoundException.forRefLogId(initialHash.asString());
    }
    return logFetcher(ctx, initial, this::fetchPageFromRefLog, RefLog::getParents);
  }

  protected RefLogHead getRefLogHead(ConnectionWrapper conn) throws SQLException {
    try (Traced ignore = trace("getRefLogHead");
        PreparedStatement psSelect =
            conn.conn().prepareStatement(SqlStatements.SELECT_REF_LOG_HEAD)) {
      psSelect.setString(1, config.getRepositoryId());
      try (ResultSet resultSet = psSelect.executeQuery()) {
        if (resultSet.next()) {
          Hash head = Hash.of(resultSet.getString(1));
          ImmutableRefLogHead.Builder refLogHead = RefLogHead.builder().refLogHead(head);
          byte[] parentsBytes = resultSet.getBytes(2);
          if (parentsBytes != null) {
            try {
              RefLogParents refLogParents = RefLogParents.parseFrom(parentsBytes);
              refLogParents
                  .getRefLogParentsInclHeadList()
                  .forEach(b -> refLogHead.addRefLogParentsInclHead(Hash.of(b)));
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException(e);
            }
          }
          return refLogHead.build();
        }
        return null;
      }
    }
  }

  @Override
  protected RefLog doFetchFromRefLog(ConnectionWrapper connection, Hash refLogId) {
    if (refLogId == null) {
      // set the current head as refLogId
      try {
        RefLogHead head = getRefLogHead(connection);
        refLogId = head != null ? head.getRefLogHead() : null;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    try (PreparedStatement ps = connection.conn().prepareStatement(SqlStatements.SELECT_REF_LOG)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, refLogId.asString());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? protoToRefLog(rs.getBytes(1)) : null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<RefLog> doFetchPageFromRefLog(ConnectionWrapper connection, List<Hash> hashes) {
    if (hashes.isEmpty()) {
      return emptyList();
    }
    String sql = sqlForManyPlaceholders(SqlStatements.SELECT_REF_LOG_MANY, hashes.size());

    try (PreparedStatement ps = connection.conn().prepareStatement(sql)) {
      ps.setString(1, config.getRepositoryId());
      for (int i = 0; i < hashes.size(); i++) {
        ps.setString(2 + i, hashes.get(i).asString());
      }

      Map<Hash, RefLog> result = new HashMap<>(hashes.size() * 2);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          RefLog entry = protoToRefLog(rs.getBytes(1));
          result.put(Objects.requireNonNull(entry).getRefLogId(), entry);
        }
      }
      return hashes.stream().map(result::get).collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void commitRefLog(
      ConnectionWrapper conn,
      long timeInMicros,
      Hash commitHash,
      NamedRef ref,
      RefLogEntry.Operation operation,
      List<Hash> sourceHashes)
      throws SQLException, ReferenceConflictException {
    RefLogHead refLogHead = getRefLogHead(conn);
    RefLogEntry newRefLog =
        writeRefLogEntry(conn, ref, refLogHead, commitHash, operation, timeInMicros, sourceHashes);
    updateRefLogHead(newRefLog, conn);
  }

  private RefLogEntry writeRefLogEntry(
      ConnectionWrapper connection,
      NamedRef ref,
      RefLogHead refLogHead,
      Hash commitHash,
      Operation operation,
      long timeInMicros,
      List<Hash> sourceHashes)
      throws ReferenceConflictException {

    ByteString newId = randomHash().asBytes();

    // Before Nessie 0.21.0: only the "head" of the ref-log is maintained, so Nessie has to read
    // the head entry of the ref-log to get the IDs of all the previous parents to fill the parents
    // in the new ref-log-entry.
    //
    // Since Nessie 0.21.0: the "head" for the ref-log and PLUS the parents of if are persisted,
    // so Nessie no longer need to read the head entries from the ref-log.
    //
    // The check of the first entry is there to ensure backwards compatibility and also
    // rolling-upgrades work.
    Stream<ByteString> newParents;
    if (refLogHead.getRefLogParentsInclHead().isEmpty()
        || !refLogHead.getRefLogParentsInclHead().get(0).equals(refLogHead.getRefLogHead())) {
      // Before Nessie 0.21.0

      newParents = Stream.of(refLogHead.getRefLogHead().asBytes());
      RefLog parentEntry = fetchFromRefLog(connection, refLogHead.getRefLogHead());
      if (parentEntry != null) {
        newParents =
            Stream.concat(
                newParents,
                parentEntry.getParents().stream()
                    .limit(config.getParentsPerRefLogEntry() - 1)
                    .map(Hash::asBytes));
      }
    } else {
      // Since Nessie 0.21.0

      newParents = refLogHead.getRefLogParentsInclHead().stream().map(Hash::asBytes);
    }
    RefType refType = ref instanceof TagName ? RefType.Tag : RefType.Branch;

    RefLogEntry.Builder entry =
        RefLogEntry.newBuilder()
            .setRefLogId(newId)
            .setRefName(ByteString.copyFromUtf8(ref.getName()))
            .setRefType(refType)
            .setCommitHash(commitHash.asBytes())
            .setOperationTime(timeInMicros)
            .setOperation(operation);
    sourceHashes.forEach(hash -> entry.addSourceHashes(hash.asBytes()));
    newParents.forEach(entry::addParents);

    RefLogEntry refLogEntry = entry.build();

    writeRefLog(connection, refLogEntry);

    return refLogEntry;
  }

  private void writeRefLog(ConnectionWrapper connection, RefLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = connection.conn().prepareStatement(SqlStatements.INSERT_REF_LOG)) {
      ps.setString(1, config.getRepositoryId());
      ps.setString(2, Hash.of(entry.getRefLogId()).asString());
      ps.setBytes(3, entry.toByteArray());
      ps.executeUpdate();
    } catch (SQLException e) {
      if (isRetryTransaction(e)) {
        throw new RetryTransactionException();
      }
      throwIfReferenceConflictException(
          e, () -> String.format("Hash collision for '%s' in ref-log", entry.getRefLogId()));
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  interface ThrowingStreamResult<R, E extends Exception> {
    @MustBeClosed
    Stream<R> process(ConnectionWrapper conn) throws E;
  }

  @MustBeClosed
  private <R, E extends Exception> Stream<R> withConnectionWrapper(ThrowingStreamResult<R, E> x)
      throws E {
    ConnectionWrapper conn = borrowConnection();
    boolean failed = true;
    try {
      @SuppressWarnings("MustBeClosedChecker")
      Stream<R> r = x.process(conn);
      failed = false;
      return r.onClose(conn::close);
    } finally {
      if (failed) {
        conn.close();
      }
    }
  }
}
