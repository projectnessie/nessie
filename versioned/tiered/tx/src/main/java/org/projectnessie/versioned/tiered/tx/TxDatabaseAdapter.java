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
package org.projectnessie.versioned.tiered.tx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentsAndState;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.tiered.adapter.CommitLogEntry;
import org.projectnessie.versioned.tiered.adapter.KeyWithBytes;
import org.projectnessie.versioned.tiered.adapter.KeyWithType;
import org.projectnessie.versioned.tiered.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.spi.DbObjectsSerializers;
import org.projectnessie.versioned.tiered.adapter.spi.TryLoopState;
import org.projectnessie.versioned.tiered.tx.TxConnectionProvider.LocalConnectionProvider;

/**
 * Transactional/relational {@link AbstractDatabaseAdapter} implementation using JDBC primitives.
 *
 * <p>Concrete implementations must at least provide the concrete column types and, if necessary,
 * provide the implementation to check for constraint-violations.l
 */
public abstract class TxDatabaseAdapter
    extends AbstractDatabaseAdapter<Connection, TxDatabaseAdapterConfig> {

  /** Value for {@link #TABLE_NAMED_REFERENCES}.{@code ref_type} for a branch. */
  protected static final String REF_TYPE_BRANCH = "b";
  /** Value for {@link #TABLE_NAMED_REFERENCES}.{@code ref_type} for a tag. */
  protected static final String REF_TYPE_TAG = "t";

  protected static final String TABLE_NAMED_REFERENCES = "named_refs";
  protected static final String TABLE_GLOBAL_STATE = "global_state";
  protected static final String TABLE_COMMIT_LOG = "commit_log";

  // SQL / DDL statements

  protected static final String CREATE_TABLE_NAMED_REFERENCES =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  ref {4},\n"
              + "  ref_type {5},\n"
              + "  hash {1},\n"
              + "  PRIMARY KEY (key_prefix, ref)\n"
              + ")",
          TABLE_NAMED_REFERENCES);
  protected static final String CREATE_TABLE_GLOBAL_STATE =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  key {3},\n"
              + "  chksum {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix, key)\n"
              + ")",
          // TODO need the history of the global-state for Nessie-GC.
          //  --> keep the previous N (20?) 'value's in this table
          //  --> move older 'value's (chunks of N=20?) to a separate table
          //  --> maintain the chunks of historic values as a linked list
          TABLE_GLOBAL_STATE);
  protected static final String CREATE_TABLE_COMMIT_LOG =
      String.format(
          "CREATE TABLE %s (\n"
              + "  key_prefix {2},\n"
              + "  hash {1},\n"
              + "  value {0},\n"
              + "  PRIMARY KEY (key_prefix, hash)\n"
              + ")",
          TABLE_COMMIT_LOG);

  // SQL / DML statements

  protected static final String SELECT_NAMED_REFERENCE =
      String.format(
          "SELECT hash FROM %s WHERE key_prefix = ? AND ref = ? AND ref_type = ?",
          TABLE_NAMED_REFERENCES);
  protected static final String SELECT_TWO_NAMED_REFERENCE =
      String.format(
          "SELECT ref, ref_type, hash FROM %s WHERE key_prefix = ? AND ref IN (?, ?)",
          TABLE_NAMED_REFERENCES);
  protected static final String SELECT_NAMED_REFERENCES =
      String.format(
          "SELECT ref_type, ref, hash FROM %s WHERE key_prefix = ?", TABLE_NAMED_REFERENCES);
  protected static final String DELETE_NAMED_REFERENCE =
      String.format(
          "DELETE FROM %s WHERE key_prefix = ? AND ref = ? AND hash = ?", TABLE_NAMED_REFERENCES);
  protected static final String INSERT_NAMED_REFERENCE =
      String.format(
          "INSERT INTO %s (key_prefix, ref, ref_type, hash) VALUES (?, ?, ?, ?)",
          TABLE_NAMED_REFERENCES);
  protected static final String UPDATE_NAMED_REFERENCE =
      String.format(
          "UPDATE %s SET hash = ? WHERE key_prefix = ? AND ref = ? AND hash = ?",
          TABLE_NAMED_REFERENCES);
  protected static final String DELETE_NAMED_REFERENCE_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_NAMED_REFERENCES);

  protected static final String SELECT_GLOBAL_STATE_MANY =
      String.format(
          "SELECT key, value FROM %s WHERE key_prefix = ? AND key IN (%%s)", TABLE_GLOBAL_STATE);
  protected static final String INSERT_GLOBAL_STATE =
      String.format(
          "INSERT INTO %s (key_prefix, key, chksum, value) VALUES (?, ?, ?, ?)",
          TABLE_GLOBAL_STATE);
  protected static final String UPDATE_GLOBAL_STATE =
      String.format(
          "UPDATE %s SET value = ?, chksum = ? WHERE key_prefix = ? AND key = ? AND chksum = ?",
          TABLE_GLOBAL_STATE);
  protected static final String UPDATE_GLOBAL_STATE_UNCOND =
      String.format(
          "UPDATE %s SET value = ?, chksum = ? WHERE key_prefix = ? AND key = ?",
          TABLE_GLOBAL_STATE);
  protected static final String DELETE_GLOBAL_STATE_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_GLOBAL_STATE);

  protected static final String SELECT_COMMIT_LOG =
      String.format("SELECT value FROM %s WHERE key_prefix = ? AND hash = ?", TABLE_COMMIT_LOG);
  protected static final String SELECT_COMMIT_LOG_MANY =
      String.format(
          "SELECT value FROM %s WHERE key_prefix = ? AND hash IN (%%s)", TABLE_COMMIT_LOG);
  protected static final String INSERT_COMMIT_LOG =
      String.format("INSERT INTO %s (key_prefix, hash, value) VALUES (?, ?, ?)", TABLE_COMMIT_LOG);
  protected static final String DELETE_COMMIT_LOG_ALL =
      String.format("DELETE FROM %s WHERE key_prefix = ?", TABLE_COMMIT_LOG);

  private static final ObjectMapper MAPPER =
      DbObjectsSerializers.register(new CBORMapper().findAndRegisterModules());

  private final TxConnectionProvider db;

  public TxDatabaseAdapter(TxDatabaseAdapterConfig config) {
    super(config);

    // get the externally configured RocksDbInstance
    TxConnectionProvider db = config.getConnectionProvider();

    if (db == null) {
      // Create a ConnectionProvider, if none has been configured externally. This is mostly used
      // for tests and benchmarks.
      db = new LocalConnectionProvider();
      db.configure(config);
      db.setupDatabase(
          allCreateTableDDL(),
          databaseSqlFormatParameters(),
          metadataUpperCase(),
          batchDDL(),
          config.getCatalog(),
          config.getSchema());
    }

    this.db = db;
  }

  @Override
  public Stream<Optional<ContentsAndState<ByteString, ByteString>>> values(
      NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys) throws ReferenceNotFoundException {
    Connection ctx = borrowConnection();
    try {
      Hash pointer = fetchNamedRefHead(ctx, ref);
      Hash hash = hashOnRef(ctx, ref, pointer, hashOnRef);

      Map<Key, ContentsAndState<ByteString, ByteString>> result = fetchValues(ctx, hash, keys);
      return keys.stream().map(result::get).map(Optional::ofNullable);
    } finally {
      releaseConnection(ctx);
    }
  }

  @Override
  public Stream<CommitLogEntry> commitLog(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException {
    Connection ctx = borrowConnection();
    boolean failed = true;
    try {
      Hash pointer = fetchNamedRefHead(ctx, ref);
      Hash hash = hashOnRef(ctx, ref, pointer, offset);

      Stream<CommitLogEntry> intLog = commitLogFetcher(ctx, hash);
      if (untilIncluding.isPresent()) {
        intLog = takeUntil(intLog, e -> e.getHash().equals(untilIncluding.get()), true);
      }

      failed = false;
      return intLog.onClose(() -> releaseConnection(ctx));
    } finally {
      if (failed) {
        releaseConnection(ctx);
      }
    }
  }

  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    Connection ctx = borrowConnection();
    try {
      return fetchNamedRefHead(ctx, ref);
    } finally {
      releaseConnection(ctx);
    }
  }

  @Override
  public Stream<WithHash<NamedRef>> namedRefs() {
    Connection ctx = borrowConnection();
    try (PreparedStatement ps = ctx.prepareStatement(SELECT_NAMED_REFERENCES)) {
      ps.setString(1, config.getKeyPrefix());

      List<WithHash<NamedRef>> result = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String type = rs.getString(1);
          String ref = rs.getString(2);
          Hash head = Hash.of(rs.getString(3));

          NamedRef namedRef = namedRefFromRow(type, ref);
          if (namedRef != null) {
            result.add(WithHash.of(head, namedRef));
          }
        }
      }

      return result.stream();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      releaseConnection(ctx);
    }
  }

  @Override
  public Stream<KeyWithType> keys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    Connection ctx = borrowConnection();
    boolean failed = true;
    try {
      Hash pointer = fetchNamedRefHead(ctx, ref);
      Hash hash = hashOnRef(ctx, ref, pointer, hashOnRef);
      Stream<KeyWithType> r = keysForCommitEntry(ctx, hash);
      failed = false;
      return r.onClose(() -> releaseConnection(ctx));
    } finally {
      if (failed) {
        releaseConnection(ctx);
      }
    }
  }

  @Override
  public Hash merge(
      NamedRef from,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      boolean commonAncestorRequired)
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
          toBranch,
          false,
          (ctx, currentHead) -> {
            Hash fromHEAD = fetchNamedRefHead(ctx, from);

            Hash toHead =
                mergeAttempt(
                    ctx,
                    from,
                    fromHEAD,
                    fromHash,
                    toBranch,
                    currentHead,
                    expectedHash,
                    h -> {},
                    commonAncestorRequired);
            return tryMoveNamedReference(ctx, toBranch, currentHead, toHead);
          },
          () ->
              mergeConflictMessage(
                  "Conflict", from, fromHash, toBranch, expectedHash, commonAncestorRequired),
          () ->
              mergeConflictMessage(
                  "Retry-failure", from, fromHash, toBranch, expectedHash, commonAncestorRequired));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Hash transplant(
      BranchName toBranch, Optional<Hash> hashOnTo, NamedRef source, List<Hash> commits)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      if (commits.isEmpty()) {
        throw new IllegalArgumentException("No hashes to transplant given.");
      }

      return opLoop(
          toBranch,
          false,
          (ctx, currentHead) -> {
            Hash sourceHead = fetchNamedRefHead(ctx, source);

            Hash targetHead =
                transplantAttempt(
                    ctx, toBranch, currentHead, hashOnTo, source, sourceHead, commits, h -> {});

            return tryMoveNamedReference(ctx, toBranch, currentHead, targetHead);
          },
          () -> transplantConflictMessage("Conflict", toBranch, hashOnTo, source, commits),
          () -> transplantConflictMessage("Retry-failure", toBranch, hashOnTo, source, commits));

    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Hash commit(
      BranchName branch,
      Optional<Hash> expectedHead,
      Map<Key, ByteString> expectedStates,
      List<KeyWithBytes> puts,
      Map<Key, ByteString> global,
      List<Key> unchanged,
      List<Key> deletes,
      Set<Key> operationsKeys,
      ByteString commitMetaSerialized)
      throws ReferenceConflictException, ReferenceNotFoundException {
    try {
      return opLoop(
          branch,
          false,
          (ctx, branchHead) -> {
            CommitLogEntry newBranchCommit =
                commitAttempt(
                    ctx,
                    branch,
                    branchHead,
                    expectedHead,
                    expectedStates,
                    puts,
                    unchanged,
                    deletes,
                    operationsKeys,
                    commitMetaSerialized);

            upsertGlobalStates(expectedStates, global, ctx);

            return tryMoveNamedReference(ctx, branch, branchHead, newBranchCommit.getHash());
          },
          () -> commitConflictMessage("Conflict", branch, expectedHead, operationsKeys),
          () -> commitConflictMessage("Retry-Failure", branch, expectedHead, operationsKeys));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Hash create(NamedRef ref, Optional<NamedRef> target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    try {
      if (ref instanceof TagName && (!target.isPresent() || !targetHash.isPresent())) {
        throw new IllegalArgumentException(
            "Tag-creation requires a target named-reference and hash.");
      }

      NamedRef realTarget = target.orElseGet(() -> BranchName.of(config.getDefaultBranch()));
      return opLoop(
          ref,
          true,
          (ctx, nullHead) -> {
            if (checkNamedRefExistence(ctx, ref)) {
              throw referenceAlreadyExists(ref);
            }

            Optional<Hash> beginning =
                targetHash.isPresent() ? targetHash : Optional.of(NO_ANCESTOR);
            Hash hash;
            if (beginning.get().equals(NO_ANCESTOR)
                && !target.isPresent()
                && ref.equals(BranchName.of(config.getDefaultBranch()))) {
              // Handle the special case when the default-branch does not exist and the current
              // request creates it. This mostly happens during tests.
              hash = NO_ANCESTOR;
            } else {
              Hash targetHead = fetchNamedRefHead(ctx, realTarget);
              hash = hashOnRef(ctx, realTarget, targetHead, beginning);
            }

            insertNewReference(ctx, ref, hash);

            return hash;
          },
          () -> createConflictMessage("Conflict", ref, realTarget, targetHash),
          () -> createConflictMessage("Retry-Failure", ref, realTarget, targetHash));
    } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      opLoop(
          ref,
          false,
          (ctx, pointer) -> {
            verifyExpectedHash(pointer, ref, hash);

            try (PreparedStatement ps = ctx.prepareStatement(DELETE_NAMED_REFERENCE)) {
              ps.setString(1, config.getKeyPrefix());
              ps.setString(2, ref.getName());
              ps.setString(3, pointer.asString());
              if (ps.executeUpdate() == 1) {
                return pointer;
              }
              return null;
            }
          },
          () -> deleteConflictMessage("Conflict", ref, hash),
          () -> deleteConflictMessage("Retry-Failure", ref, hash));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assign(
      NamedRef ref, Optional<Hash> expectedHash, NamedRef assignTo, Optional<Hash> assignToHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      opLoop(
          ref,
          true,
          (ctx, pointer) -> {
            List<Hash> refAndTo = fetchTwoNamedRefHeads(ctx, ref, assignTo);
            pointer = refAndTo.get(0);
            Hash assignHead = refAndTo.get(1);

            verifyExpectedHash(pointer, ref, expectedHash);
            Hash assignToHead = hashOnRef(ctx, assignTo, assignHead, assignToHash);

            return tryMoveNamedReference(ctx, ref, pointer, assignToHead);
          },
          () -> assignConflictMessage("Conflict", ref, expectedHash, assignTo, assignToHash),
          () -> assignConflictMessage("Retry-Failure", ref, expectedHash, assignTo, assignToHash));
    } catch (ReferenceNotFoundException | ReferenceConflictException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Stream<Diff<ByteString>> diff(
      NamedRef from, Optional<Hash> hashOnFrom, NamedRef to, Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException {
    Connection ctx = borrowConnection();
    try {
      List<Hash> refAndTo = fetchTwoNamedRefHeads(ctx, from, to);
      Hash fromHead = refAndTo.get(0);
      Hash toHead = refAndTo.get(1);

      return buildDiff(ctx, from, fromHead, hashOnFrom, to, toHead, hashOnTo);
    } finally {
      releaseConnection(ctx);
    }
  }

  @Override
  public void initializeRepo() {
    Connection ctx = borrowConnection();
    try {
      if (!checkNamedRefExistence(ctx, BranchName.of(config.getDefaultBranch()))) {
        insertNewReference(ctx, BranchName.of(config.getDefaultBranch()), NO_ANCESTOR);

        txCommit(ctx);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      releaseConnection(ctx);
    }
  }

  @Override
  public void reinitializeRepo() {
    Connection ctx = borrowConnection();
    try {
      try (PreparedStatement ps = ctx.prepareStatement(DELETE_NAMED_REFERENCE_ALL)) {
        ps.setString(1, config.getKeyPrefix());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = ctx.prepareStatement(DELETE_GLOBAL_STATE_ALL)) {
        ps.setString(1, config.getKeyPrefix());
        ps.executeUpdate();
      }
      try (PreparedStatement ps = ctx.prepareStatement(DELETE_COMMIT_LOG_ALL)) {
        ps.setString(1, config.getKeyPrefix());
        ps.executeUpdate();
      }

      txCommit(ctx);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      releaseConnection(ctx);
    }

    initializeRepo();
  }

  @Override
  public void close() throws Exception {
    db.close();
  }

  // /////////////////////////////////////////////////////////////////////////////////////////////
  // Transactional DatabaseAdapter subclass API (protected)
  // /////////////////////////////////////////////////////////////////////////////////////////////

  /** Implementation notes: transactional implementations must rollback changes. */
  private void releaseConnection(Connection ctx) {
    try {
      try {
        ctx.rollback();
      } finally {
        ctx.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected byte[] serialize(Object entry) {
    try {
      return MAPPER.writeValueAsBytes(entry);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("SameParameterValue")
  protected <T> T deserialize(byte[] bytes, Class<T> type) {
    try {
      return MAPPER.readValue(bytes, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Connection borrowConnection() {
    try {
      return db.borrowConnection();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void txCommit(Connection ctx) {
    try {
      ctx.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void txRollback(Connection ctx) {
    try {
      ctx.rollback();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  public interface LoopOp {
    /**
     * Applies an operation within a CAS-loop. The implementation gets the current global-state and
     * must return an updated global-state with a different global-id.
     */
    Hash apply(Connection ctx, Hash targetRefHead) throws VersionStoreException, SQLException;
  }

  /** This is the actual CAS-loop, which applies an operation onto a named-ref. */
  protected Hash opLoop(
      NamedRef ref,
      boolean createRef,
      LoopOp op,
      Supplier<String> conflictErrorMessage,
      Supplier<String> retryErrorMessage)
      throws VersionStoreException {
    // TODO this loop should be bounded (time + #attempts) - using the same logic as
    //  NonTxDatabaseAdapter
    Connection ctx = borrowConnection();
    try (TryLoopState tryState = newTryLoopState(retryErrorMessage)) {
      while (true) {
        Hash pointer = createRef ? null : fetchNamedRefHead(ctx, ref);

        Hash newHead;
        try {
          newHead = op.apply(ctx, pointer);
        } catch (RetryTransactionException e) {
          txRollback(ctx);
          tryState.retry();
          continue;
        } catch (SQLException e) {
          if (isRetryTransaction(e)) {
            txRollback(ctx);
            tryState.retry();
            continue;
          }
          throwIfReferenceConflictException(e, conflictErrorMessage);
          throw new RuntimeException(e);
        }

        // The operation succeeded, if it returns a non-null hash value.
        if (newHead != null) {
          txCommit(ctx);
          return tryState.success(newHead);
        }

        txRollback(ctx);
        tryState.retry();
      }
    } finally {
      releaseConnection(ctx);
    }
  }

  protected List<Hash> fetchTwoNamedRefHeads(Connection ctx, NamedRef ref1, NamedRef ref2)
      throws ReferenceNotFoundException {
    try (PreparedStatement ps = ctx.prepareStatement(SELECT_TWO_NAMED_REFERENCE)) {
      ps.setString(1, config.getKeyPrefix());
      ps.setString(2, ref1.getName());
      ps.setString(3, ref2.getName());
      // "SELECT ref, ref_type, hash FROM %s WHERE key_prefix = ? AND ref IN (?, ?)"
      Hash[] hashes = new Hash[2];
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String name = rs.getString(1);
          String type = rs.getString(2);
          NamedRef ref = namedRefFromRow(type, name);
          Hash hash = Hash.of(rs.getString(3));

          if (hashes[0] == null && ref.equals(ref1)) {
            hashes[0] = hash;
          }
          if (hashes[1] == null && ref.equals(ref2)) {
            hashes[1] = hash;
          }
        }

        if (hashes[0] == null) {
          throw refNotFound(ref1);
        }
        if (hashes[1] == null) {
          throw refNotFound(ref2);
        }

        return Arrays.asList(hashes);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
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

  /**
   * Retrieves the current HEAD for a reference, throws a {@link ReferenceNotFoundException}, it the
   * reference does not exist.
   */
  protected Hash fetchNamedRefHead(Connection c, NamedRef ref) throws ReferenceNotFoundException {
    try (PreparedStatement ps = c.prepareStatement(SELECT_NAMED_REFERENCE)) {
      ps.setString(1, config.getKeyPrefix());
      ps.setString(2, ref.getName());
      ps.setString(3, refType(ref));
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Hash.of(rs.getString(1));
        }
        throw refNotFound(ref);
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

  protected void insertNewReference(Connection ctx, NamedRef ref, Hash hash)
      throws ReferenceAlreadyExistsException, SQLException {
    try (PreparedStatement ps = ctx.prepareStatement(INSERT_NAMED_REFERENCE)) {
      ps.setString(1, config.getKeyPrefix());
      ps.setString(2, ref.getName());
      ps.setString(3, refType(ref));
      ps.setString(4, hash.asString());
      ps.executeUpdate();
    } catch (SQLException e) {
      if (isIntegrityConstraintViolation(e)) {
        throw referenceAlreadyExists(ref);
      }
      throw e;
    }
  }

  protected String refType(NamedRef ref) {
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
   * Upserts the global-states to {@code global} and checks that the current values are equal to
   * those in {@code expectedStates}.
   */
  protected void upsertGlobalStates(
      Map<Key, ByteString> expectedStates, Map<Key, ByteString> global, Connection ctx)
      throws SQLException {
    try (PreparedStatement psUpdateUncond = ctx.prepareStatement(UPDATE_GLOBAL_STATE_UNCOND);
        PreparedStatement psUpdate = ctx.prepareStatement(UPDATE_GLOBAL_STATE);
        PreparedStatement psInsert = ctx.prepareStatement(INSERT_GLOBAL_STATE)) {
      for (Entry<Key, ByteString> e : global.entrySet()) {
        byte[] glob = e.getValue().toByteArray();
        @SuppressWarnings("UnstableApiUsage")
        String newHash = newHasher().putBytes(glob).hash().toString();
        String key = DbObjectsSerializers.flattenKey(e.getKey());
        ByteString expected = expectedStates.getOrDefault(e.getKey(), ByteString.EMPTY);

        PreparedStatement ps = expected.isEmpty() ? psUpdateUncond : psUpdate;
        ps.setBytes(1, glob);
        ps.setString(2, newHash);
        ps.setString(3, config.getKeyPrefix());
        ps.setString(4, key);
        if (!expected.isEmpty()) {
          @SuppressWarnings("UnstableApiUsage")
          String expectedHash =
              newHasher().putBytes(expected.asReadOnlyByteBuffer()).hash().toString();
          ps.setString(5, expectedHash);
        }

        if (ps.executeUpdate() == 0) {
          psInsert.setString(1, config.getKeyPrefix());
          psInsert.setString(2, key);
          psInsert.setString(3, newHash);
          psInsert.setBytes(4, glob);
          psInsert.executeUpdate();
        }
      }
    }
  }

  @Override
  protected Map<Key, ByteString> fetchGlobalStates(Connection ctx, Collection<Key> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    String placeholders =
        IntStream.range(0, keys.size()).mapToObj(x -> "?").collect(Collectors.joining(", "));
    String sql = String.format(SELECT_GLOBAL_STATE_MANY, placeholders);

    try (PreparedStatement ps = ctx.prepareStatement(sql)) {
      ps.setString(1, config.getKeyPrefix());
      int i = 2;
      for (Key key : keys) {
        ps.setString(i++, DbObjectsSerializers.flattenKey(key));
      }

      Map<Key, ByteString> result = new HashMap<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Key key = DbObjectsSerializers.fromFlattened(rs.getString(1));
          ByteString val = UnsafeByteOperations.unsafeWrap(rs.getBytes(2));
          result.put(key, val);
        }
      }
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(Connection c, Hash hash) {
    try (PreparedStatement ps = c.prepareStatement(TxDatabaseAdapter.SELECT_COMMIT_LOG)) {
      ps.setString(1, config.getKeyPrefix());
      ps.setString(2, hash.asString());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? deserialize(rs.getBytes(1), CommitLogEntry.class) : null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(Connection c, List<Hash> hashes) {
    String placeholders =
        IntStream.range(0, hashes.size()).mapToObj(x -> "?").collect(Collectors.joining(", "));
    String sql = String.format(SELECT_COMMIT_LOG_MANY, placeholders);

    try (PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, config.getKeyPrefix());
      for (int i = 0; i < hashes.size(); i++) {
        ps.setString(2 + i, hashes.get(i).asString());
      }

      Map<Hash, CommitLogEntry> result = new HashMap<>(hashes.size() * 2);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          CommitLogEntry entry = deserialize(rs.getBytes(1), CommitLogEntry.class);
          result.put(entry.getHash(), entry);
        }
      }
      return hashes.stream().map(result::get).collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeIndividualCommit(Connection c, CommitLogEntry entry)
      throws ReferenceConflictException {
    try (PreparedStatement ps = c.prepareStatement(INSERT_COMMIT_LOG)) {
      ps.setString(1, config.getKeyPrefix());
      ps.setString(2, entry.getHash().asString());
      ps.setBytes(3, serialize(entry));
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
  protected void writeIndividualCommits(Connection c, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    int cnt = 0;
    try (PreparedStatement ps = c.prepareStatement(INSERT_COMMIT_LOG)) {
      for (CommitLogEntry e : entries) {
        ps.setString(1, config.getKeyPrefix());
        ps.setString(2, e.getHash().asString());
        ps.setBytes(3, serialize(e));
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
                      .map(x -> "'" + x.getHash().asString() + "'")
                      .collect(Collectors.joining(", "))));
      throw new RuntimeException(e);
    }
  }

  protected void throwIfReferenceConflictException(SQLException e, Supplier<String> message)
      throws ReferenceConflictException {
    if (isIntegrityConstraintViolation(e)) {
      throw new ReferenceConflictException(message.get(), e);
    }
  }

  protected static final String RETRY_SQL_STATE_COCKROACH = "40001";
  protected static final String CONSTRAINT_VIOLATION_SQL_STATE = "23505";

  protected boolean isIntegrityConstraintViolation(Throwable e) {
    if (e instanceof SQLException) {
      SQLException sqlException = (SQLException) e;
      return sqlException instanceof SQLIntegrityConstraintViolationException
          // e.g. H2
          || sqlException.getErrorCode() == 23505
          // e.g. Postgres
          || CONSTRAINT_VIOLATION_SQL_STATE.equals(sqlException.getSQLState());
    }
    return false;
  }

  /**
   * Check whether the {@link SQLException} indicates a "retry hint". This can happen when there is
   * too much contention, see <a
   * href="https://www.cockroachlabs.com/docs/v21.1/transaction-retry-error-reference.html#retry_write_too_old">Cockroach's
   * Transaction Retry Error Reference</a>.
   */
  protected boolean isRetryTransaction(SQLException e) {
    return RETRY_SQL_STATE_COCKROACH.equals(e.getSQLState());
  }

  /**
   * Updates the HEAD of the given {@code ref} from {@code expectedHead} to {@code newHead}. Returns
   * {@code newHead}, if successful, and {@code null} if not.
   */
  protected Hash tryMoveNamedReference(
      Connection ctx, NamedRef ref, Hash expectedHead, Hash newHead) {
    try (PreparedStatement ps = ctx.prepareStatement(UPDATE_NAMED_REFERENCE)) {
      ps.setString(1, newHead.asString());
      ps.setString(2, config.getKeyPrefix());
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

  /**
   * Provides a map of table name to create-table-DDL. The DDL statements are processed by {@link
   * java.text.MessageFormat} to inject the parameters returned by {@link
   * #databaseSqlFormatParameters()}, which is for example used to have the "proper", database
   * specific column types.
   *
   * <p>Names of the tables are defined by the constants defined in this class that start with
   * {@code TABLE_}, for example {@link #TABLE_COMMIT_LOG}.
   */
  protected Map<String, List<String>> allCreateTableDDL() {
    return ImmutableMap.<String, List<String>>builder()
        .put(TABLE_GLOBAL_STATE, Collections.singletonList(CREATE_TABLE_GLOBAL_STATE))
        .put(TABLE_NAMED_REFERENCES, Collections.singletonList(CREATE_TABLE_NAMED_REFERENCES))
        .put(TABLE_COMMIT_LOG, Collections.singletonList(CREATE_TABLE_COMMIT_LOG))
        .build();
  }

  /**
   * Get database-specific 'strings' like column definitions for 'BLOB' column types.
   *
   * <ul>
   *   <li>Index #0: column-type string for a 'BLOB' column.
   *   <li>Index #1: column-type string for the string representation of a {@link Hash}
   *   <li>Index #2: column-type string for key-prefix
   *   <li>Index #3: column-type string for the string representation of a {@link Key}
   *   <li>Index #4: column-type string for the string representation of a {@link NamedRef}
   *   <li>Index #5: column-type string for the named-reference-type (single char)
   * </ul>
   */
  protected abstract List<String> databaseSqlFormatParameters();

  /** Whether the database/JDBC-driver require schema-metadata-queries require upper-case names. */
  protected boolean metadataUpperCase() {
    return true;
  }

  /** Whether this implementation shall use bates for DDL operations to create tables. */
  protected boolean batchDDL() {
    return false;
  }
}
