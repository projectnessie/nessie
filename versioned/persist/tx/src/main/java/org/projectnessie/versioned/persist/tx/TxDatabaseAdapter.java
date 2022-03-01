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

import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.assignConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.commitConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.createConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.deleteConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.mergeConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.newHasher;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceAlreadyExists;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.referenceNotFound;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.repoDescUpdateConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.transplantConflictMessage;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.verifyExpectedHash;
import static org.projectnessie.versioned.persist.adapter.spi.Traced.trace;
import static org.projectnessie.versioned.persist.adapter.spi.TryLoopState.newTryLoopState;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToCommitLogEntry;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToRepoDescription;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.toProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.persist.adapter.CommitAttempt;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.ContentIdWithType;
import org.projectnessie.versioned.persist.adapter.ContentVariantSupplier;
import org.projectnessie.versioned.persist.adapter.Difference;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.Traced;
import org.projectnessie.versioned.persist.adapter.spi.TryLoopState;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;

/**
 * Transactional/relational {@link AbstractDatabaseAdapter} implementation using JDBC primitives.
 *
 * <p>Concrete implementations must at least provide the concrete column types and, if necessary,
 * provide the implementation to check for constraint-violations.
 */
public abstract class TxDatabaseAdapter
    extends AbstractDatabaseAdapter<Connection, TxDatabaseAdapterConfig> {

  /** Value for {@link SqlStatements#TABLE_NAMED_REFERENCES}.{@code ref_type} for a branch. */
  protected static final String REF_TYPE_BRANCH = "b";
  /** Value for {@link SqlStatements#TABLE_NAMED_REFERENCES}.{@code ref_type} for a tag. */
  protected static final String REF_TYPE_TAG = "t";

  private final TxConnectionProvider<?> db;

  public TxDatabaseAdapter(
      TxDatabaseAdapterConfig config,
      TxConnectionProvider<?> db,
      ContentVariantSupplier contentVariantSupplier) {
    super(config, contentVariantSupplier);

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
    Connection conn = borrowConnection();
    try {
      return hashOnRef(conn, namedReference, hashOnReference);
    } finally {
      releaseConnection(conn);
    }
  }

  @Override
  public Map<Key, ContentAndState<ByteString>> values(
      Hash commit, Collection<Key> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    Connection conn = borrowConnection();
    try {
      return fetchValues(conn, commit, keys, keyFilter);
    } finally {
      releaseConnection(conn);
    }
  }

  @Override
  public Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException {
    Connection conn = borrowConnection();
    boolean failed = true;
    try {
      Stream<CommitLogEntry> intLog = readCommitLogStream(conn, offset);

      failed = false;
      return intLog.onClose(() -> releaseConnection(conn));
    } finally {
      if (failed) {
        releaseConnection(conn);
      }
    }
  }

  @Override
  public ReferenceInfo<ByteString> namedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null");

    Connection conn = borrowConnection();
    try {
      ReferenceInfo<ByteString> refInfo = fetchNamedRef(conn, ref);
      Hash defaultBranchHead = namedRefsDefaultBranchHead(conn, params);

      Stream<ReferenceInfo<ByteString>> refs = Stream.of(refInfo);

      return namedRefsFilterAndEnhance(conn, params, defaultBranchHead, refs)
          .findFirst()
          .orElseThrow(() -> referenceNotFound(ref));
    } finally {
      releaseConnection(conn);
    }
  }

  public Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    Preconditions.checkNotNull(params, "Parameter for GetNamedRefsParams must not be null.");
    Preconditions.checkArgument(
        namedRefsAnyRetrieves(params), "Must retrieve branches or tags or both.");

    Connection conn = borrowConnection();
    boolean failed = true;
    try {

      Hash defaultBranchHead = namedRefsDefaultBranchHead(conn, params);

      Stream<ReferenceInfo<ByteString>> refs = fetchNamedRefs(conn);

      refs = namedRefsFilterAndEnhance(conn, params, defaultBranchHead, refs);

      failed = false;

      return refs.onClose(() -> releaseConnection(conn));
    } finally {
      if (failed) {
        releaseConnection(conn);
      }
    }
  }

  @Override
  public Stream<KeyWithType> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    Connection conn = borrowConnection();
    boolean failed = true;
    try {
      Stream<KeyWithType> r = keysForCommitEntry(conn, commit, keyFilter);
      failed = false;
      return r.onClose(() -> releaseConnection(conn));
    } finally {
      if (failed) {
        releaseConnection(conn);
      }
    }
  }

  @Override
  public Hash merge(
      Hash from,
      BranchName toBranch,
      Optional<Hash> expectedHead,
      Function<ByteString, ByteString> updateCommitMetadata)
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
      return opLoop(
          "merge",
          toBranch,
          false,
          (conn, currentHead) -> {
            long timeInMicros = commitTimeInMicros();

            Hash toHead =
                mergeAttempt(
                    conn,
                    timeInMicros,
                    from,
                    toBranch,
                    expectedHead,
                    currentHead,
                    h -> {},
                    h -> {},
                    updateCommitMetadata);
            Hash resultHash = tryMoveNamedReference(conn, toBranch, currentHead, toHead);

            commitRefLog(
                conn,
                timeInMicros,
                toHead,
                toBranch,
                AdapterTypes.RefLogEntry.Operation.MERGE,
                Collections.singletonList(from));

            return resultHash;
          },
          () -> mergeConflictMessage("Conflict", from, toBranch, expectedHead),
          () -> mergeConflictMessage("Retry-failure", from, toBranch, expectedHead));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Hash transplant(
      BranchName targetBranch,
      Optional<Hash> expectedHead,
      List<Hash> commits,
      Function<ByteString, ByteString> updateCommitMetadata)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return opLoop(
          "transplant",
          targetBranch,
          false,
          (conn, currentHead) -> {
            long timeInMicros = commitTimeInMicros();

            Hash targetHead =
                transplantAttempt(
                    conn,
                    timeInMicros,
                    targetBranch,
                    expectedHead,
                    currentHead,
                    commits,
                    h -> {},
                    h -> {},
                    updateCommitMetadata);

            Hash resultHash = tryMoveNamedReference(conn, targetBranch, currentHead, targetHead);

            commitRefLog(
                conn,
                timeInMicros,
                targetHead,
                targetBranch,
                AdapterTypes.RefLogEntry.Operation.TRANSPLANT,
                commits);

            return resultHash;
          },
          () -> transplantConflictMessage("Conflict", targetBranch, expectedHead, commits),
          () -> transplantConflictMessage("Retry-failure", targetBranch, expectedHead, commits));

    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash commit(CommitAttempt commitAttempt)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return opLoop(
          "commit",
          commitAttempt.getCommitToBranch(),
          false,
          (conn, branchHead) -> {
            long timeInMicros = commitTimeInMicros();

            CommitLogEntry newBranchCommit =
                commitAttempt(conn, timeInMicros, branchHead, commitAttempt, h -> {});

            upsertGlobalStates(commitAttempt, conn, newBranchCommit.getCreatedTime());

            Hash resultHash =
                tryMoveNamedReference(
                    conn, commitAttempt.getCommitToBranch(), branchHead, newBranchCommit.getHash());

            commitRefLog(
                conn,
                timeInMicros,
                newBranchCommit.getHash(),
                commitAttempt.getCommitToBranch(),
                AdapterTypes.RefLogEntry.Operation.COMMIT,
                Collections.emptyList());

            return resultHash;
          },
          () ->
              commitConflictMessage(
                  "Conflict", commitAttempt.getCommitToBranch(), commitAttempt.getExpectedHead()),
          () ->
              commitConflictMessage(
                  "Retry-Failure",
                  commitAttempt.getCommitToBranch(),
                  commitAttempt.getExpectedHead()));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Hash create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException {
    try {
      return opLoop(
          "createRef",
          ref,
          true,
          (conn, nullHead) -> {
            if (checkNamedRefExistence(conn, ref.getName())) {
              throw referenceAlreadyExists(ref);
            }

            Hash hash = target;
            if (hash == null) {
              // Special case: Don't validate, if the 'target' parameter is null.
              // This is mostly used for tests that re-create the default-branch.
              hash = NO_ANCESTOR;
            }

            validateHashExists(conn, hash);

            insertNewReference(conn, ref, hash);

            commitRefLog(
                conn,
                commitTimeInMicros(),
                hash,
                ref,
                AdapterTypes.RefLogEntry.Operation.CREATE_REFERENCE,
                Collections.emptyList());

            return hash;
          },
          () -> createConflictMessage("Conflict", ref, target),
          () -> createConflictMessage("Retry-Failure", ref, target));
    } catch (ReferenceAlreadyExistsException | ReferenceNotFoundException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      opLoop(
          "deleteRef",
          reference,
          false,
          (conn, pointer) -> {
            verifyExpectedHash(pointer, reference, expectedHead);

            Hash commitHash = fetchNamedRefHead(conn, reference);
            try (Traced ignore = trace("deleteRefInDb");
                PreparedStatement ps =
                    conn.prepareStatement(SqlStatements.DELETE_NAMED_REFERENCE)) {
              ps.setString(1, config.getRepositoryId());
              ps.setString(2, reference.getName());
              ps.setString(3, pointer.asString());
              if (ps.executeUpdate() != 1) {
                return null;
              }
            }

            commitRefLog(
                conn,
                commitTimeInMicros(),
                commitHash,
                reference,
                AdapterTypes.RefLogEntry.Operation.DELETE_REFERENCE,
                Collections.emptyList());

            return pointer;
          },
          () -> deleteConflictMessage("Conflict", reference, expectedHead),
          () -> deleteConflictMessage("Retry-Failure", reference, expectedHead));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assign(NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      opLoop(
          "assignRef",
          assignee,
          true,
          (conn, assigneeHead) -> {
            assigneeHead = fetchNamedRefHead(conn, assignee);

            verifyExpectedHash(assigneeHead, assignee, expectedHead);

            if (!NO_ANCESTOR.equals(assignTo) && fetchFromCommitLog(conn, assignTo) == null) {
              throw referenceNotFound(assignTo);
            }

            Hash resultHash = tryMoveNamedReference(conn, assignee, assigneeHead, assignTo);

            commitRefLog(
                conn,
                commitTimeInMicros(),
                assignTo,
                assignee,
                AdapterTypes.RefLogEntry.Operation.ASSIGN_REFERENCE,
                Collections.singletonList(assigneeHead));

            return resultHash;
          },
          () -> assignConflictMessage("Conflict", assignee, expectedHead, assignTo),
          () -> assignConflictMessage("Retry-Failure", assignee, expectedHead, assignTo));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException {
    Connection conn = borrowConnection();
    try {
      return buildDiff(conn, from, to, keyFilter);
    } finally {
      releaseConnection(conn);
    }
  }

  @Override
  public void initializeRepo(String defaultBranchName) {
    Connection conn = borrowConnection();
    try {
      if (!checkNamedRefExistence(conn, BranchName.of(defaultBranchName))) {
        // note: no need to initialize the repo-description

        insertNewReference(conn, BranchName.of(defaultBranchName), NO_ANCESTOR);

        Hash newRefLogId =
            writeRefLogEntry(
                conn,
                BranchName.of(defaultBranchName),
                NO_ANCESTOR,
                NO_ANCESTOR,
                AdapterTypes.RefLogEntry.Operation.CREATE_REFERENCE,
                commitTimeInMicros(),
                Collections.emptyList());
        insertRefLogHead(newRefLogId, conn);

        txCommit(conn);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      releaseConnection(conn);
    }
  }

  @Override
  public void eraseRepo() {
    Connection conn = borrowConnection();
    try {
      try (PreparedStatement ps = conn.prepareStatement(SqlStatements.DELETE_NAMED_REFERENCE_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.prepareStatement(SqlStatements.DELETE_GLOBAL_STATE_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.prepareStatement(SqlStatements.DELETE_COMMIT_LOG_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.prepareStatement(SqlStatements.DELETE_KEY_LIST_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.prepareStatement(SqlStatements.DELETE_REF_LOG_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = conn.prepareStatement(SqlStatements.DELETE_REF_LOG_HEAD_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }
      try (PreparedStatement ps =
          conn.prepareStatement(SqlStatements.DELETE_REPO_DESCRIPTIONE_ALL)) {
        ps.setString(1, config.getRepositoryId());
        ps.executeUpdate();
      }

      txCommit(conn);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      releaseConnection(conn);
    }
  }

  @Override
  public Stream<ContentIdWithType> globalKeys(ToIntFunction<ByteString> contentTypeExtractor) {
    Connection conn = borrowConnection();
    return JdbcSelectSpliterator.buildStream(
            conn,
            SqlStatements.SELECT_GLOBAL_STATE_ALL,
            ps -> ps.setString(1, config.getRepositoryId()),
            (rs) -> {
              ContentId contentId = ContentId.of(rs.getString(1));
              byte[] value = rs.getBytes(2);
              byte type =
                  (byte) contentTypeExtractor.applyAsInt(UnsafeByteOperations.unsafeWrap(value));

              return ContentIdWithType.of(contentId, type);
            })
        .onClose(() -> releaseConnection(conn));
  }

  @Override
  public Optional<ContentIdAndBytes> globalContent(
      ContentId contentId, ToIntFunction<ByteString> contentTypeExtractor) {
    Connection conn = borrowConnection();
    try {
      try (Traced ignore = trace("globalContent");
          PreparedStatement ps =
              conn.prepareStatement(String.format(SqlStatements.SELECT_GLOBAL_STATE_MANY, "?"))) {
        ps.setString(1, config.getRepositoryId());
        ps.setString(2, contentId.getId());
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return Optional.of(globalContentFromRow(contentTypeExtractor, rs));
          }
        }
      }
      return Optional.empty();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      releaseConnection(conn);
    }
  }

  @Override
  public Stream<ContentIdAndBytes> globalContent(
      Set<ContentId> keys, ToIntFunction<ByteString> contentTypeExtractor) {
    // 1. Fetch the global states,
    // 1.1. filter the requested keys + content-ids
    // 1.2. extract the current state from the GLOBAL_STATE table

    // 1. Fetch the global states,
    Connection conn = borrowConnection();
    return JdbcSelectSpliterator.buildStream(
            conn,
            sqlForManyPlaceholders(SqlStatements.SELECT_GLOBAL_STATE_MANY_WITH_LOGS, keys.size()),
            ps -> {
              ps.setString(1, config.getRepositoryId());
              int i = 2;
              for (ContentId key : keys) {
                ps.setString(i++, key.getId());
              }
            },
            (rs) -> {
              ContentIdAndBytes global = globalContentFromRow(contentTypeExtractor, rs);
              if (!keys.contains(global.getContentId())) {
                // 1.1. filter the requested keys + content-ids
                return null;
              }

              // 1.2. extract the current state from the GLOBAL_STATE table
              return global;
            })
        .onClose(() -> releaseConnection(conn));
  }

  @Override
  public Stream<RefLog> refLog(Hash offset) throws RefLogNotFoundException {
    Connection conn = borrowConnection();
    boolean failed = true;
    try {
      Stream<RefLog> intLog = readRefLogStream(conn, offset);
      failed = false;
      return intLog.onClose(() -> releaseConnection(conn));
    } finally {
      if (failed) {
        releaseConnection(conn);
      }
    }
  }

  private ContentIdAndBytes globalContentFromRow(
      ToIntFunction<ByteString> contentTypeExtractor, ResultSet rs) throws SQLException {
    ContentId cid = ContentId.of(rs.getString(1));
    ByteString value = UnsafeByteOperations.unsafeWrap(rs.getBytes(2));
    byte type = (byte) contentTypeExtractor.applyAsInt(value);
    return ContentIdAndBytes.of(cid, type, value);
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // Transactional DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Convenience for {@link AbstractDatabaseAdapter#hashOnRef(Object, NamedRef, Optional, Hash)
   * hashOnRef(conn, ref, fetchNamedRefHead(conn, ref.getReference()))}.
   */
  protected Hash hashOnRef(Connection conn, NamedRef reference, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    return hashOnRef(conn, reference, hashOnRef, fetchNamedRefHead(conn, reference));
  }

  /** Implementation notes: transactional implementations must rollback changes. */
  private void releaseConnection(Connection conn) {
    try {
      try {
        conn.rollback();
      } finally {
        conn.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyWithType entry) {
    return toProto(entry).getSerializedSize();
  }

  protected Connection borrowConnection() {
    try {
      return db.borrowConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void txCommit(Connection conn) {
    try {
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void txRollback(Connection conn) {
    try {
      conn.rollback();
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
    Hash apply(Connection conn, Hash targetRefHead) throws VersionStoreException, SQLException;
  }

  /**
   * This is the actual CAS-ish-loop, which applies an operation onto a named-ref.
   *
   * <p>Each CAS-loop-iteration fetches the current HEAD of the named reference and calls {@link
   * LoopOp#apply(Connection, Hash)}. If {@code apply()} throws a {@link RetryTransactionException}
   * (see {@link #isRetryTransaction(SQLException)}), the current JDBC transaction is rolled back
   * and the next loop-iteration starts, unless the retry-policy allows no more retries, in which
   * case a {@link ReferenceRetryFailureException} with the message from {@code retryErrorMessage}
   * is thrown. If the thrown exception is a {@link #isIntegrityConstraintViolation(Throwable)}, the
   * exception is re-thrown as a {@link ReferenceConflictException} with an appropriate message from
   * the {@code conflictErrorMessage} supplier.
   *
   * <p>If {@link LoopOp#apply(Connection, Hash)} completes normally and returns a non-{@code null}
   * hash, the JDBC transaction is committed and the hash returned from this function. If {@code
   * apply()} returns {@code null}, the operation is retried, unless the retry-policy allows no more
   * retries, in which * case a {@link ReferenceRetryFailureException} with the message from {@code
   * retryErrorMessage} * is thrown.
   *
   * <p>Uses {@link TryLoopState} for retry handling.
   *
   * @param namedReference the named reference on which the Nessie operation works
   * @param createRef flag, whether this ia a "create-named-reference" operation, which skips the
   *     retrieval of the current HEAD
   * @param loopOp the implementation of the Nessie operation
   * @param conflictErrorMessage message producer to represent an unresolvable conflict in the data
   * @param retryErrorMessage message producer to represent that no more retries will happen
   * @see LoopOp#apply(Connection, Hash)
   */
  protected Hash opLoop(
      String opName,
      NamedRef namedReference,
      boolean createRef,
      LoopOp loopOp,
      Supplier<String> conflictErrorMessage,
      Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    Connection conn = borrowConnection();

    try (TryLoopState tryState =
        newTryLoopState(
            opName,
            ts ->
                repoDescUpdateConflictMessage(
                    String.format(
                        "%s after %d retries, %d ms",
                        retryErrorMessage.get(),
                        ts.getRetries(),
                        ts.getDuration(TimeUnit.MILLISECONDS))),
            this::tryLoopStateCompletion,
            config)) {
      while (true) {
        Hash pointer = createRef ? null : fetchNamedRefHead(conn, namedReference);

        Hash newHead;
        try {
          newHead = loopOp.apply(conn, pointer);
        } catch (RetryTransactionException e) {
          txRollback(conn);
          tryState.retry();
          continue;
        } catch (SQLException e) {
          if (isRetryTransaction(e)) {
            txRollback(conn);
            tryState.retry();
            continue;
          }
          throwIfReferenceConflictException(e, conflictErrorMessage);
          throw new RuntimeException(e);
        }

        // The operation succeeded, if it returns a non-null hash value.
        if (newHead != null) {
          txCommit(conn);
          return tryState.success(newHead);
        }

        txRollback(conn);
        tryState.retry();
      }
    } finally {
      releaseConnection(conn);
    }
  }

  protected Stream<ReferenceInfo<ByteString>> fetchNamedRefs(Connection conn) {
    return JdbcSelectSpliterator.buildStream(
        conn,
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

  /** Similar to {@link #fetchNamedRefHead(Connection, NamedRef)}, but just checks for existence. */
  protected boolean checkNamedRefExistence(Connection c, NamedRef ref) {
    try {
      fetchNamedRefHead(c, ref);
      return true;
    } catch (ReferenceNotFoundException e) {
      return false;
    }
  }

  /** Similar to {@link #fetchNamedRefHead(Connection, NamedRef)}, but just checks for existence. */
  protected boolean checkNamedRefExistence(Connection c, String refName) {
    try (Traced ignore = trace("checkNamedRefExistence");
        PreparedStatement ps = c.prepareStatement(SqlStatements.SELECT_NAMED_REFERENCE_NAME)) {
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
  protected Hash fetchNamedRefHead(Connection c, NamedRef ref) throws ReferenceNotFoundException {
    try (Traced ignore = trace("fetchNamedRefHead");
        PreparedStatement ps = c.prepareStatement(SqlStatements.SELECT_NAMED_REFERENCE)) {
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

  protected ReferenceInfo<ByteString> fetchNamedRef(Connection c, String ref)
      throws ReferenceNotFoundException {
    try (Traced ignore = trace("fetchNamedRef");
        PreparedStatement ps = c.prepareStatement(SqlStatements.SELECT_NAMED_REFERENCE_ANY)) {
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

  protected NamedRef namedRefFromRow(String type, String ref) {
    switch (type) {
      case REF_TYPE_BRANCH:
        return BranchName.of(ref);
      case REF_TYPE_TAG:
        return TagName.of(ref);
      default:
        return null;
    }
  }

  protected void insertNewReference(Connection conn, NamedRef ref, Hash hash)
      throws ReferenceAlreadyExistsException, SQLException {
    try (Traced ignore = trace("insertNewReference");
        PreparedStatement ps = conn.prepareStatement(SqlStatements.INSERT_NAMED_REFERENCE)) {
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

  protected String referenceTypeDiscriminator(NamedRef ref) {
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
  private Hash namedRefsDefaultBranchHead(Connection conn, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    if (namedRefsRequiresBaseReference(params)) {
      Preconditions.checkNotNull(params.getBaseReference(), "Base reference name missing.");
      return fetchNamedRefHead(conn, params.getBaseReference());
    }
    return null;
  }

  /**
   * Upserts the global-states to {@code global} and checks that the current values are equal to
   * those in {@code expectedStates}.
   *
   * <p>The implementation is a bit verbose (b/c JDBC is verbose).
   *
   * <p>The algorithm works like this:
   *
   * <ol>
   *   <li>Try to update the existing {@code Key + Content.id} rows in {@value
   *       SqlStatements#TABLE_GLOBAL_STATE}
   *       <ol>
   *         <li>Perform a {@code SELECT} on {@value SqlStatements#TABLE_GLOBAL_STATE} fetching all
   *             rows for the requested keys.
   *         <li>Filter those rows that match the content-ids
   *         <li>Perform the {@code UPDATE} on {@value SqlStatements#TABLE_GLOBAL_STATE}. If the
   *             user requested to compare the current-global-state, perform a conditional update.
   *             Otherwise blindly update it.
   *       </ol>
   *   <li>Insert the not updated (= new) rows into {@value SqlStatements#TABLE_GLOBAL_STATE}
   * </ol>
   */
  protected void upsertGlobalStates(CommitAttempt commitAttempt, Connection conn, long newCreatedAt)
      throws SQLException {
    if (commitAttempt.getGlobal().isEmpty()) {
      return;
    }

    String sql =
        sqlForManyPlaceholders(
            SqlStatements.SELECT_GLOBAL_STATE_MANY_WITH_LOGS, commitAttempt.getGlobal().size());

    Set<ContentId> newKeys = new HashSet<>(commitAttempt.getGlobal().keySet());

    try (Traced ignore = trace("upsertGlobalStates");
        PreparedStatement psSelect = conn.prepareStatement(sql);
        PreparedStatement psUpdate = conn.prepareStatement(SqlStatements.UPDATE_GLOBAL_STATE);
        PreparedStatement psUpdateUnconditional =
            conn.prepareStatement(SqlStatements.UPDATE_GLOBAL_STATE_UNCOND)) {

      // 1.2. SELECT returns all already existing rows --> UPDATE

      psSelect.setString(1, config.getRepositoryId());
      int i = 2;
      for (ContentId cid : commitAttempt.getGlobal().keySet()) {
        psSelect.setString(i++, cid.getId());
      }

      try (ResultSet rs = psSelect.executeQuery()) {
        while (rs.next()) {
          ContentId contentId = ContentId.of(rs.getString(1));

          // 1.3. Use only those rows from the SELECT against the GLOBAL_STATE table that match the
          // key + content-id we need.

          // content-id exists -> not a new row
          newKeys.remove(contentId);

          ByteString newState = commitAttempt.getGlobal().get(contentId);

          ByteString expected =
              commitAttempt
                  .getExpectedStates()
                  .getOrDefault(contentId, Optional.empty())
                  .orElse(ByteString.EMPTY);

          // 1.4. Perform the UPDATE. If an expected-state is present, perform a "conditional"
          // update that compares the existing value.
          // Note: always perform the conditional-UPDATE on the checksum field as a concurrent
          // update might have already changed the GLOBAL_STATE.

          byte[] newStateBytes = newState.toByteArray();
          @SuppressWarnings("UnstableApiUsage")
          String newHash = newHasher().putBytes(newStateBytes).hash().toString();

          // Perform a conditional update, if the client asked us to do so.
          try (Traced ignored = trace("upsertGlobalStatesPerform")) {
            PreparedStatement ps = expected.isEmpty() ? psUpdateUnconditional : psUpdate;
            ps.setBytes(1, newStateBytes);
            ps.setString(2, newHash);
            ps.setLong(3, newCreatedAt);
            ps.setString(4, config.getRepositoryId());
            ps.setString(5, contentId.getId());
            if (!expected.isEmpty()) {
              // Only perform a conditional update, if the client asked us to do so...

              @SuppressWarnings("UnstableApiUsage")
              String expectedHash =
                  newHasher().putBytes(expected.asReadOnlyByteBuffer()).hash().toString();
              ps.setString(6, expectedHash);
            }

            // Check whether the UPDATE worked. If not -> reference-conflict
            if (ps.executeUpdate() != 1) {
              // No need to continue, just throw a legit constraint-violation that will be
              // converted to a "proper ReferenceConflictException" later up in the stack.
              throw newIntegrityConstraintViolationException();
            }
          }
        }
      }

      // 2. INSERT all global-state values for those keys that were not handled above.
      if (!newKeys.isEmpty()) {
        try (Traced ignored = trace("upsertGlobalStatesInsert")) {
          PreparedStatement psInsert = conn.prepareStatement(SqlStatements.INSERT_GLOBAL_STATE);
          for (ContentId contentId : newKeys) {
            ByteString newGlob = commitAttempt.getGlobal().get(contentId);
            byte[] newGlobBytes = newGlob.toByteArray();
            @SuppressWarnings("UnstableApiUsage")
            String newHash = newHasher().putBytes(newGlobBytes).hash().toString();

            if (contentId == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "No contentId in CommitAttempt.keyToContent content-id '%s'", contentId));
            }

            psInsert.setString(1, config.getRepositoryId());
            psInsert.setString(2, contentId.getId());
            psInsert.setString(3, newHash);
            psInsert.setBytes(4, newGlobBytes);
            psInsert.setLong(5, newCreatedAt);
            psInsert.executeUpdate();
          }
        }
      }
    }
  }

  protected String sqlForManyPlaceholders(String sql, int num) {
    String placeholders =
        IntStream.range(0, num).mapToObj(x -> "?").collect(Collectors.joining(", "));
    return String.format(sql, placeholders);
  }

  @Override
  protected Map<ContentId, ByteString> doFetchGlobalStates(
      Connection conn, Set<ContentId> contentIds) {
    if (contentIds.isEmpty()) {
      return Collections.emptyMap();
    }

    String sql = sqlForManyPlaceholders(SqlStatements.SELECT_GLOBAL_STATE_MANY, contentIds.size());

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, config.getRepositoryId());
      int i = 2;
      for (ContentId cid : contentIds) {
        ps.setString(i++, cid.getId());
      }

      Map<ContentId, ByteString> result = new HashMap<>();
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
  protected CommitLogEntry doFetchFromCommitLog(Connection c, Hash hash) {
    try (PreparedStatement ps = c.prepareStatement(SqlStatements.SELECT_COMMIT_LOG)) {
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
  protected List<CommitLogEntry> doFetchPageFromCommitLog(Connection c, List<Hash> hashes) {
    String sql = sqlForManyPlaceholders(SqlStatements.SELECT_COMMIT_LOG_MANY, hashes.size());

    try (PreparedStatement ps = c.prepareStatement(sql)) {
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
  protected void doWriteIndividualCommit(Connection c, CommitLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = c.prepareStatement(SqlStatements.INSERT_COMMIT_LOG)) {
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
  protected void doWriteMultipleCommits(Connection c, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    writeMany(
        c,
        SqlStatements.INSERT_COMMIT_LOG,
        entries,
        e -> e.getHash().asString(),
        e -> toProto(e).toByteArray());
  }

  @Override
  protected void doWriteKeyListEntities(Connection c, List<KeyListEntity> newKeyListEntities) {
    try {
      writeMany(
          c,
          SqlStatements.INSERT_KEY_LIST,
          newKeyListEntities,
          e -> e.getId().asString(),
          e -> toProto(e.getKeys()).toByteArray());
    } catch (ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  protected <T> void writeMany(
      Connection c,
      String sqlInsert,
      List<T> entries,
      Function<T, String> idRetriever,
      Function<T, byte[]> serializer)
      throws ReferenceConflictException {
    int cnt = 0;
    try (PreparedStatement ps = c.prepareStatement(sqlInsert)) {
      for (T e : entries) {
        ps.setString(1, config.getRepositoryId());
        ps.setString(2, idRetriever.apply(e));
        ps.setBytes(3, serializer.apply(e));
        ps.addBatch();
        cnt++;
        if (cnt == config.getBatchSize()) {
          ps.executeBatch();
          cnt = 0;
        }
      }
      if (cnt > 0) {
        ps.executeBatch();
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
  protected Stream<KeyListEntity> doFetchKeyLists(Connection c, List<Hash> keyListsIds) {
    try (Traced ignore = trace("doFetchKeyLists.stream")) {
      return JdbcSelectSpliterator.buildStream(
          c,
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
      Connection conn, NamedRef ref, Hash expectedHead, Hash newHead) {
    try (Traced ignore = trace("tryMoveNamedReference");
        PreparedStatement ps = conn.prepareStatement(SqlStatements.UPDATE_NAMED_REFERENCE)) {
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
    Connection conn = borrowConnection();
    try {
      return fetchRepositoryDescription(conn);
    } finally {
      releaseConnection(conn);
    }
  }

  private RepoDescription fetchRepositoryDescription(Connection conn) {
    try {
      return protoToRepoDescription(fetchRepositoryDescriptionInternal(conn));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] fetchRepositoryDescriptionInternal(Connection conn) throws SQLException {
    try (Traced ignore = trace("fetchRepositoryDescriptionInternal");
        PreparedStatement ps = conn.prepareStatement(SqlStatements.SELECT_REPO_DESCRIPTION)) {
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
                    conn.prepareStatement(SqlStatements.INSERT_REPO_DESCRIPTION)) {
                  ps.setString(1, config.getRepositoryId());
                  ps.setBytes(2, toProto(updated).toByteArray());
                  if (ps.executeUpdate() == 0) {
                    return null;
                  }
                }
              } else {
                try (PreparedStatement ps =
                    conn.prepareStatement(SqlStatements.UPDATE_REPO_DESCRIPTION)) {
                  ps.setBytes(1, toProto(updated).toByteArray());
                  ps.setString(2, config.getRepositoryId());
                  ps.setBytes(3, currentBytes);
                  if (ps.executeUpdate() == 0) {
                    return null;
                  }
                }
              }
            }

            return NO_ANCESTOR;
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
    /** Column-type string for the string representation of a {@link Key}. */
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

  private AdapterTypes.RefLogEntry buildRefLogEntry(
      NamedRef ref,
      Hash parentRefLogId,
      Hash commitHash,
      AdapterTypes.RefLogEntry.Operation operation,
      long timeInMicros,
      List<Hash> sourceHashes,
      RefLog parentEntry) {
    Stream<Hash> newParents = Stream.of(parentRefLogId);

    if (parentEntry != null) {
      newParents =
          Stream.concat(
              newParents,
              parentEntry.getParents().stream().limit(config.getParentsPerRefLogEntry() - 1));
    }

    AdapterTypes.RefLogEntry.RefType refType =
        ref instanceof TagName
            ? AdapterTypes.RefLogEntry.RefType.Tag
            : AdapterTypes.RefLogEntry.RefType.Branch;

    AdapterTypes.RefLogEntry.Builder entry =
        AdapterTypes.RefLogEntry.newBuilder()
            .setRefLogId(randomHash().asBytes())
            .setRefName(ByteString.copyFromUtf8(ref.getName()))
            .setRefType(refType)
            .setCommitHash(commitHash.asBytes())
            .setOperationTime(timeInMicros)
            .setOperation(operation);
    sourceHashes.forEach(hash -> entry.addSourceHashes(hash.asBytes()));
    newParents.forEach(p -> entry.addParents(p.asBytes()));
    return entry.build();
  }

  protected void updateRefLogHead(Hash newRefLogId, Hash parentRefLogId, Connection conn)
      throws SQLException {
    try (Traced ignore = trace("updateRefLogHead");
        PreparedStatement psUpdate = conn.prepareStatement(SqlStatements.UPDATE_REF_LOG_HEAD)) {
      psUpdate.setString(1, newRefLogId.asString());
      psUpdate.setString(2, config.getRepositoryId());
      psUpdate.setString(3, parentRefLogId.asString());
      if (psUpdate.executeUpdate() != 1) {
        // retry the transaction with rebasing the parent id.
        throw new RetryTransactionException();
      }
    }
  }

  protected void insertRefLogHead(Hash newRefLogId, Connection conn) throws SQLException {
    try (Traced ignore = trace("insertRefLogHead");
        PreparedStatement selectStatement =
            conn.prepareStatement(SqlStatements.SELECT_REF_LOG_HEAD)) {
      selectStatement.setString(1, config.getRepositoryId());
      // insert if the table is empty
      if (!selectStatement.executeQuery().next()) {
        PreparedStatement psUpdate = conn.prepareStatement(SqlStatements.INSERT_REF_LOG_HEAD);
        psUpdate.setString(1, config.getRepositoryId());
        psUpdate.setString(2, newRefLogId.asString());
        if (psUpdate.executeUpdate() != 1) {
          // No need to continue, just throw a legit constraint-violation that will be
          // converted to a "proper ReferenceConflictException" later up in the stack.
          throw newIntegrityConstraintViolationException();
        }
      }
    }
  }

  protected Hash getRefLogHead(Connection conn) throws SQLException {
    try (Traced ignore = trace("getRefLogHead");
        PreparedStatement psSelect = conn.prepareStatement(SqlStatements.SELECT_REF_LOG_HEAD)) {
      psSelect.setString(1, config.getRepositoryId());
      ResultSet resultSet = psSelect.executeQuery();
      if (resultSet.next()) {
        return Hash.of(resultSet.getString(1));
      }
      return null;
    }
  }

  @Override
  protected RefLog doFetchFromRefLog(Connection connection, Hash refLogId) {
    if (refLogId == null) {
      // set the current head as refLogId
      try {
        refLogId = getRefLogHead(connection);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    try (PreparedStatement ps = connection.prepareStatement(SqlStatements.SELECT_REF_LOG)) {
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
  protected List<RefLog> doFetchPageFromRefLog(Connection connection, List<Hash> hashes) {
    String sql = sqlForManyPlaceholders(SqlStatements.SELECT_REF_LOG_MANY, hashes.size());

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
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
      Connection conn,
      long timeInMicros,
      Hash commitHash,
      NamedRef ref,
      AdapterTypes.RefLogEntry.Operation operation,
      List<Hash> sourceHashes)
      throws SQLException, ReferenceConflictException {
    Hash parentId = getRefLogHead(conn);
    Hash newRefLogId =
        writeRefLogEntry(conn, ref, parentId, commitHash, operation, timeInMicros, sourceHashes);
    updateRefLogHead(newRefLogId, parentId, conn);
  }

  private Hash writeRefLogEntry(
      Connection connection,
      NamedRef ref,
      Hash parentRefLogId,
      Hash commitHash,
      AdapterTypes.RefLogEntry.Operation operation,
      long timeInMicros,
      List<Hash> sourceHashes)
      throws ReferenceConflictException {

    RefLog parentEntry = fetchFromRefLog(connection, parentRefLogId);
    AdapterTypes.RefLogEntry refLogEntry =
        buildRefLogEntry(
            ref, parentRefLogId, commitHash, operation, timeInMicros, sourceHashes, parentEntry);

    writeRefLog(connection, refLogEntry);

    return Hash.of(refLogEntry.getRefLogId());
  }

  private void writeRefLog(Connection connection, AdapterTypes.RefLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = connection.prepareStatement(SqlStatements.INSERT_REF_LOG)) {
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
}
