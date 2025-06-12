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
package org.projectnessie.versioned.storage.jdbc;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.projectnessie.versioned.storage.common.persist.ObjTypes.objTypeByName;
import static org.projectnessie.versioned.storage.common.util.Closing.closeMultiple;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.deserializeObjId;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeObjId;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.ADD_REFERENCE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COLS_OBJS_ALL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_OBJ_REFERENCED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_OBJ_VERS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.DELETE_OBJ;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.DELETE_OBJ_CONDITIONAL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.DELETE_OBJ_REFERENCED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.DELETE_OBJ_REFERENCED_NULL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.FETCH_OBJ_TYPE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.FIND_OBJS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.FIND_OBJS_TYPED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.FIND_REFERENCES;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.MARK_REFERENCE_AS_DELETED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.MAX_BATCH_SIZE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.PURGE_REFERENCE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.REFS_CREATED_AT_COND;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.REFS_EXTENDED_INFO_COND;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.SCAN_OBJS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.SCAN_OBJS_ALL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.UPDATE_OBJS_REFERENCED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.UPDATE_REFERENCE_POINTER;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.jdbc.serializers.ObjSerializer;
import org.projectnessie.versioned.storage.jdbc.serializers.ObjSerializers;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
abstract class AbstractJdbcPersist implements Persist {

  private final StoreConfig config;
  private final DatabaseSpecific databaseSpecific;
  private final String storeObjSql;
  private final Map<String, Integer> storeObjSqlParams;
  private final int fetchSize;

  AbstractJdbcPersist(DatabaseSpecific databaseSpecific, int fetchSize, StoreConfig config) {
    this.config = config;
    this.fetchSize = fetchSize;
    this.databaseSpecific = databaseSpecific;
    this.storeObjSqlParams = buildStoreObjSqlParams();
    this.storeObjSql = buildStoreObjSql();
  }

  private Map<String, Integer> buildStoreObjSqlParams() {
    Builder<String, Integer> params = ImmutableMap.builder();
    int i = 1;
    params.put(COL_REPO_ID, i++);
    for (String col : COLS_OBJS_ALL.keySet()) {
      params.put(col, i++);
    }
    return params.build();
  }

  private String buildStoreObjSql() {
    return "INSERT INTO "
        + TABLE_OBJS
        + " ("
        + String.join(", ", storeObjSqlParams.keySet())
        + ") VALUES ("
        + storeObjSqlParams.keySet().stream().map(c -> "?").collect(joining(", "))
        + ")";
  }

  @Nonnull
  @Override
  public String name() {
    return JdbcBackendFactory.NAME;
  }

  @Override
  @Nonnull
  public StoreConfig config() {
    return config;
  }

  protected final Reference findReference(@Nonnull Connection conn, @Nonnull String name) {
    return findReferences(conn, new String[] {name})[0];
  }

  @Nonnull
  protected final Reference[] findReferences(@Nonnull Connection conn, @Nonnull String[] names) {
    Object2IntHashMap<String> nameToIndex =
        new Object2IntHashMap<>(200, Hashing.DEFAULT_LOAD_FACTOR, -1);
    Reference[] r = new Reference[names.length];
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < names.length; i++) {
      String name = names[i];
      if (name != null) {
        keys.add(name);
        nameToIndex.put(name, i);
      }
    }

    if (keys.isEmpty()) {
      return r;
    }

    try (PreparedStatement ps =
        conn.prepareStatement(sqlSelectMultiple(FIND_REFERENCES, keys.size()))) {
      int idx = 1;
      ps.setString(idx++, config.repositoryId());
      for (String key : keys) {
        ps.setString(idx++, key);
      }
      ps.setFetchSize(fetchSize);
      try (ResultSet rs = ps.executeQuery()) {
        rs.setFetchSize(fetchSize);
        while (rs.next()) {
          Reference ref = JdbcSerde.deserializeReference(rs);
          int i = nameToIndex.getValue(ref.name());
          if (i != -1) {
            r[i] = ref;
          }
        }
        return r;
      }
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  @Nonnull
  protected final Reference addReference(@Nonnull Connection conn, @Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    String sql = databaseSpecific.wrapInsert(ADD_REFERENCE);
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, config.repositoryId());
      ps.setString(2, reference.name());
      serializeObjId(ps, 3, reference.pointer(), databaseSpecific);
      ps.setBoolean(4, reference.deleted());
      if (reference.createdAtMicros() != 0L) {
        ps.setLong(5, reference.createdAtMicros());
      } else {
        ps.setNull(5, Types.BIGINT);
      }
      serializeObjId(ps, 6, reference.extendedInfoObj(), databaseSpecific);
      byte[] previous = serializePreviousPointers(reference.previousPointers());
      if (previous != null) {
        ps.setBytes(7, previous);
      } else {
        ps.setNull(7, Types.BINARY);
      }

      if (ps.executeUpdate() != 1) {
        throw new RefAlreadyExistsException(fetchReference(reference.name()));
      }

      return reference;
    } catch (SQLException e) {
      if (databaseSpecific.isConstraintViolation(e)) {
        throw new RefAlreadyExistsException(fetchReference(reference.name()));
      }
      throw unhandledSQLException(e);
    }
  }

  @Nonnull
  protected final Reference markReferenceAsDeleted(
      @Nonnull Connection conn, @Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try (PreparedStatement ps =
        conn.prepareStatement(referencesDml(MARK_REFERENCE_AS_DELETED, reference))) {
      int idx = 1;
      ps.setBoolean(idx++, true);
      ps.setString(idx++, config().repositoryId());
      ps.setString(idx++, reference.name());
      serializeObjId(ps, idx++, reference.pointer(), databaseSpecific);
      ps.setBoolean(idx++, false);
      long createdAtMicros = reference.createdAtMicros();
      if (createdAtMicros != 0L) {
        ps.setLong(idx++, createdAtMicros);
      }
      ObjId extendedInfoObj = reference.extendedInfoObj();
      if (extendedInfoObj != null) {
        serializeObjId(ps, idx, extendedInfoObj, databaseSpecific);
      }

      if (ps.executeUpdate() != 1) {
        Reference ref = findReference(conn, reference.name());
        if (ref == null) {
          throw new RefNotFoundException(reference);
        }
        throw new RefConditionFailedException(ref);
      }

      return reference.withDeleted(true);
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected final void purgeReference(@Nonnull Connection conn, @Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try (PreparedStatement ps = conn.prepareStatement(referencesDml(PURGE_REFERENCE, reference))) {
      int idx = 1;
      ps.setString(idx++, config().repositoryId());
      ps.setString(idx++, reference.name());
      serializeObjId(ps, idx++, reference.pointer(), databaseSpecific);
      ps.setBoolean(idx++, true);
      long createdAtMicros = reference.createdAtMicros();
      if (createdAtMicros != 0L) {
        ps.setLong(idx++, createdAtMicros);
      }
      ObjId extendedInfoObj = reference.extendedInfoObj();
      if (extendedInfoObj != null) {
        serializeObjId(ps, idx, extendedInfoObj, databaseSpecific);
      }

      if (ps.executeUpdate() != 1) {
        Reference ref = findReference(conn, reference.name());
        if (ref == null) {
          throw new RefNotFoundException(reference);
        }
        throw new RefConditionFailedException(ref);
      }
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  @Nonnull
  protected final Reference updateReferencePointer(
      @Nonnull Connection conn, @Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    try (PreparedStatement ps =
        conn.prepareStatement(referencesDml(UPDATE_REFERENCE_POINTER, reference))) {
      int idx = 1;
      serializeObjId(ps, idx++, newPointer, databaseSpecific);
      Reference updated = reference.forNewPointer(newPointer, config);
      byte[] previous = serializePreviousPointers(updated.previousPointers());
      if (previous != null) {
        ps.setBytes(idx++, previous);
      } else {
        ps.setNull(idx++, Types.BINARY);
      }

      ps.setString(idx++, config().repositoryId());
      ps.setString(idx++, reference.name());
      serializeObjId(ps, idx++, reference.pointer(), databaseSpecific);
      ps.setBoolean(idx++, false);
      long createdAtMicros = reference.createdAtMicros();
      if (createdAtMicros != 0L) {
        ps.setLong(idx++, createdAtMicros);
      }
      ObjId extendedInfoObj = reference.extendedInfoObj();
      if (extendedInfoObj != null) {
        serializeObjId(ps, idx, extendedInfoObj, databaseSpecific);
      }

      if (ps.executeUpdate() != 1) {
        Reference ref = findReference(conn, reference.name());
        if (ref == null) {
          throw new RefNotFoundException(reference);
        }
        throw new RefConditionFailedException(ref);
      }

      return updated;
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  private String referencesDml(String sql, Reference reference) {
    String createdAtCond = reference.createdAtMicros() != 0L ? "=?" : " IS NULL";
    String extendedInfoCond = reference.extendedInfoObj() != null ? "=?" : " IS NULL";
    return sql.replace(REFS_CREATED_AT_COND, createdAtCond)
        .replace(REFS_EXTENDED_INFO_COND, extendedInfoCond);
  }

  protected <T extends Obj> T fetchTypedObj(
      Connection conn, ObjId id, ObjType type, Class<T> typeClass) throws ObjNotFoundException {
    T obj = fetchTypedObjsIfExist(conn, new ObjId[] {id}, type, typeClass)[0];

    if (obj == null) {
      throw new ObjNotFoundException(id);
    }

    return obj;
  }

  protected ObjType fetchObjType(@Nonnull Connection conn, @Nonnull ObjId id)
      throws ObjNotFoundException {
    try (PreparedStatement ps = conn.prepareStatement(sqlSelectMultiple(FETCH_OBJ_TYPE, 1))) {
      ps.setString(1, config.repositoryId());
      serializeObjId(ps, 2, id, databaseSpecific);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          String objType = rs.getString(1);
          return objTypeByName(objType);
        }
      }
      throw new ObjNotFoundException(id);
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  @Nonnull
  protected final <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull Connection conn,
      @Nonnull ObjId[] ids,
      ObjType type,
      @SuppressWarnings("unused") Class<T> typeClass) {
    Object2IntHashMap<ObjId> idToIndex =
        new Object2IntHashMap<>(200, Hashing.DEFAULT_LOAD_FACTOR, -1);
    @SuppressWarnings("unchecked")
    T[] r = (T[]) Array.newInstance(typeClass, ids.length);
    List<ObjId> keys = new ArrayList<>();
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null) {
        keys.add(id);
        idToIndex.put(id, i);
      }
    }

    if (keys.isEmpty()) {
      return r;
    }

    String sql = type == null ? FIND_OBJS : FIND_OBJS_TYPED;
    sql = sqlSelectMultiple(sql, keys.size());

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      int idx = 1;
      ps.setString(idx++, config.repositoryId());
      for (ObjId key : keys) {
        serializeObjId(ps, idx++, key, databaseSpecific);
      }
      if (type != null) {
        ps.setString(idx, type.name());
      }

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Obj obj = deserializeObj(rs);
          int i = idToIndex.getValue(obj.id());
          if (i != -1) {
            @SuppressWarnings("unchecked")
            T typed = (T) obj;
            r[i] = typed;
          }
        }

        return r;
      }
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  private Obj deserializeObj(ResultSet rs) throws SQLException {
    ObjId id = deserializeObjId(rs, COL_OBJ_ID);
    String objType = rs.getString(COL_OBJ_TYPE);
    String versionToken = rs.getString(COL_OBJ_VERS);
    long referenced = rs.getLong(COL_OBJ_REFERENCED);
    if (rs.wasNull()) {
      referenced = -1;
    }
    ObjType type = objTypeByName(objType);
    ObjSerializer<Obj> serializer = ObjSerializers.forType(type);
    return serializer.deserialize(rs, type, id, referenced, versionToken);
  }

  protected final boolean storeObj(
      @Nonnull Connection conn, @Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return upsertObjs(conn, new Obj[] {obj}, ignoreSoftSizeRestrictions, true)[0];
  }

  @Nonnull
  protected final boolean[] storeObjs(@Nonnull Connection conn, @Nonnull Obj[] objs)
      throws ObjTooLargeException {
    return upsertObjs(conn, objs, false, true);
  }

  protected final Void updateObj(@Nonnull Connection conn, @Nonnull Obj obj)
      throws ObjTooLargeException {
    updateObjs(conn, new Obj[] {obj});
    return null;
  }

  protected final Void updateObjs(@Nonnull Connection conn, @Nonnull Obj[] objs)
      throws ObjTooLargeException {
    upsertObjs(conn, objs, false, false);
    return null;
  }

  protected final boolean deleteWithReferenced(@Nonnull Connection conn, @Nonnull Obj obj) {
    var referenced = obj.referenced();
    var referencedPresent = referenced != -1L;
    try (PreparedStatement ps =
        conn.prepareStatement(
            referencedPresent ? DELETE_OBJ_REFERENCED : DELETE_OBJ_REFERENCED_NULL)) {
      ps.setString(1, config.repositoryId());
      serializeObjId(ps, 2, obj.id(), databaseSpecific);
      if (referencedPresent) {
        ps.setLong(3, referenced);
      }
      return ps.executeUpdate() == 1;
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected final boolean deleteConditional(@Nonnull Connection conn, @Nonnull UpdateableObj obj) {
    try (PreparedStatement ps = conn.prepareStatement(DELETE_OBJ_CONDITIONAL)) {
      ps.setString(1, config.repositoryId());
      serializeObjId(ps, 2, obj.id(), databaseSpecific);
      ps.setString(3, obj.type().name());
      ps.setString(4, obj.versionToken());
      return ps.executeUpdate() == 1;
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected final boolean updateConditional(
      @Nonnull Connection conn, @Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    ObjId id = expected.id();
    checkArgument(id != null && id.equals(newValue.id()));
    checkArgument(expected.type().equals(newValue.type()));
    checkArgument(!expected.versionToken().equals(newValue.versionToken()));

    // See comment in upsertObjs() why this is implemented this way.
    return deleteConditional(conn, expected)
        && upsertObjs(conn, new Obj[] {newValue}, false, true)[0];
  }

  @Nonnull
  private boolean[] upsertObjs(
      @Nonnull Connection conn,
      @Nonnull Obj[] objs,
      boolean ignoreSoftSizeRestrictions,
      boolean insert)
      throws ObjTooLargeException {
    if (!insert) {
      // Sadly an INSERT INTO ... ON CONFLICT DO UPDATE SET ... does not work with parameters in the
      // UPDATE SET clause. Since the JDBC connection is configured with auto-commit=false, we can
      // just DELETE the updates to be upserted and INSERT them again.
      deleteObjs(
          conn, stream(objs).map(obj -> obj == null ? null : obj.id()).toArray(ObjId[]::new));
    }

    boolean[] r = new boolean[objs.length];

    List<ObjId> updateReferenced = new ArrayList<>();

    upsertObjsWrite(conn, objs, ignoreSoftSizeRestrictions, r, updateReferenced);

    if (!updateReferenced.isEmpty()) {
      upsertObjsReferenced(conn, updateReferenced);
    }

    return r;
  }

  private void upsertObjsWrite(
      Connection conn,
      Obj[] objs,
      boolean ignoreSoftSizeRestrictions,
      boolean[] r,
      List<ObjId> updateReferenced)
      throws ObjTooLargeException {
    try (PreparedStatement ps = conn.prepareStatement(databaseSpecific.wrapInsert(storeObjSql))) {
      Int2IntHashMap batchIndexToObjIndex =
          new Int2IntHashMap(objs.length * 2, Hashing.DEFAULT_LOAD_FACTOR, -1);

      Consumer<int[]> batchResultHandler =
          updated -> {
            for (int i = 0; i < updated.length; i++) {
              int objIndex = batchIndexToObjIndex.get(i);
              if (updated[i] == 1) {
                r[objIndex] = true;
              } else if (updated[i] != 0) {
                throw new IllegalStateException(
                    "driver returned unexpected value for a batch update: " + updated[i]);
              } else {
                updateReferenced.add(objs[objIndex].id());
              }
            }
          };

      long referenced = config.currentTimeMicros();

      int batchIndex = 0;
      for (int i = 0; i < objs.length; i++) {
        Obj obj = objs[i];
        if (obj == null) {
          continue;
        }

        ObjId id = obj.id();
        ObjType type = obj.type();

        int incrementalIndexSizeLimit =
            ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
        int indexSizeLimit =
            ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();

        checkArgument(id != null, "Obj to store must have a non-null ID");

        ps.setString(storeObjSqlParams.get(COL_REPO_ID), config.repositoryId());
        serializeObjId(ps, storeObjSqlParams.get(COL_OBJ_ID), id, databaseSpecific);
        ps.setString(storeObjSqlParams.get(COL_OBJ_TYPE), type.name());
        Optional<String> versionToken = UpdateableObj.extractVersionToken(obj);
        if (versionToken.isPresent()) {
          ps.setString(storeObjSqlParams.get(COL_OBJ_VERS), versionToken.get());
        } else {
          ps.setNull(storeObjSqlParams.get(COL_OBJ_VERS), Types.VARCHAR);
        }
        if (obj.referenced() == -1L) {
          // -1 is a sentinel for AbstractBasePersistTests.deleteWithReferenced()
          ps.setNull(storeObjSqlParams.get(COL_OBJ_REFERENCED), Types.BIGINT);
        } else {
          ps.setLong(storeObjSqlParams.get(COL_OBJ_REFERENCED), referenced);
        }

        ObjSerializer<Obj> serializer = ObjSerializers.forType(type);
        serializer.serialize(
            ps,
            obj,
            incrementalIndexSizeLimit,
            indexSizeLimit,
            storeObjSqlParams::get,
            databaseSpecific);

        for (ObjSerializer<?> other : ObjSerializers.ALL_SERIALIZERS) {
          if (serializer != other) {
            other.setNull(ps, storeObjSqlParams::get, databaseSpecific);
          }
        }

        batchIndexToObjIndex.put(batchIndex++, i);
        ps.addBatch();

        if (batchIndex == MAX_BATCH_SIZE) {
          batchIndex = 0;
          batchResultHandler.accept(ps.executeBatch());
        }
      }

      if (batchIndex > 0) {
        batchResultHandler.accept(ps.executeBatch());
      }

    } catch (SQLException e) {
      if (databaseSpecific.isConstraintViolation(e)) {
        throw new UnsupportedOperationException(
            "The database should support a functionality like PostgreSQL's "
                + "'ON CONFLICT DO NOTHING' for INSERT statements. For H2, enable the "
                + "PostgreSQL Compatibility Mode.");
      }
      throw unhandledSQLException(e);
    }
  }

  private void upsertObjsReferenced(Connection conn, List<ObjId> updateReferenced) {
    try (PreparedStatement ps = conn.prepareStatement(UPDATE_OBJS_REFERENCED)) {
      long referenced = config.currentTimeMicros();

      int batchIndex = 0;
      for (ObjId id : updateReferenced) {
        ps.setLong(1, referenced);
        ps.setString(2, config.repositoryId());
        serializeObjId(ps, 3, id, databaseSpecific);
        ps.addBatch();

        if (batchIndex++ == MAX_BATCH_SIZE) {
          batchIndex = 0;
          ps.executeBatch();
        }
      }

      if (batchIndex > 0) {
        ps.executeBatch();
      }
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected final void deleteObj(@Nonnull Connection conn, @Nonnull ObjId id) {
    try (PreparedStatement ps = conn.prepareStatement(DELETE_OBJ)) {
      ps.setString(1, config.repositoryId());
      serializeObjId(ps, 2, id, databaseSpecific);

      ps.executeUpdate();
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected final void deleteObjs(@Nonnull Connection conn, @Nonnull ObjId[] ids) {
    if (ids.length == 0) {
      return;
    }

    try (PreparedStatement ps = conn.prepareStatement(DELETE_OBJ)) {
      int batchSize = 0;

      for (ObjId id : ids) {
        if (id == null) {
          continue;
        }
        ps.setString(1, config.repositoryId());
        serializeObjId(ps, 2, id, databaseSpecific);
        ps.addBatch();

        if (++batchSize == MAX_BATCH_SIZE) {
          batchSize = 0;
          ps.executeBatch();
        }
      }

      if (batchSize > 0) {
        ps.executeBatch();
      }

    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected CloseableIterator<Obj> scanAllObjects(Connection conn, Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(conn, returnedObjTypes);
  }

  @VisibleForTesting
  static String sqlSelectMultiple(String sql, int count) {
    if (count == 1) {
      return sql;
    }
    StringBuilder marks = new StringBuilder(sql.length() + 50);
    int idx = sql.indexOf("(?)");
    checkArgument(idx > 0, "SQL does not contain (?) placeholder: %s", sql);
    marks.append(sql, 0, idx).append("(?");
    for (int i = 1; i < count; i++) {
      marks.append(",?");
    }
    marks.append(')').append(sql, idx + 3, sql.length());
    return marks.toString();
  }

  @FunctionalInterface
  interface ThrowingConsumer<T> {
    void accept(T t) throws SQLException;
  }

  private static String scanSql(Set<ObjType> returnedObjTypes) {
    if (returnedObjTypes.isEmpty()) {
      return SCAN_OBJS_ALL;
    }
    return sqlSelectMultiple(SCAN_OBJS, returnedObjTypes.size());
  }

  private class ScanAllObjectsIterator extends ResultSetIterator<Obj> {
    ScanAllObjectsIterator(Connection conn, Set<ObjType> returnedObjTypes) {
      super(
          conn,
          scanSql(returnedObjTypes),
          ps -> {
            int idx = 1;
            ps.setString(idx++, config.repositoryId());
            if (!returnedObjTypes.isEmpty()) {
              for (ObjType returnedObjType : returnedObjTypes) {
                ps.setString(idx++, returnedObjType.name());
              }
            }
          });
    }

    @Override
    protected Obj mapToObj(ResultSet rs) throws SQLException {
      return deserializeObj(rs);
    }
  }

  private abstract class ResultSetIterator<R> extends AbstractIterator<R>
      implements CloseableIterator<R> {

    private final Connection conn;
    private final PreparedStatement ps;
    private final ResultSet rs;

    @SuppressWarnings("SqlSourceToSinkFlow")
    ResultSetIterator(Connection conn, String sql, ThrowingConsumer<PreparedStatement> preparer) {
      this.conn = conn;

      try {
        ps = conn.prepareStatement(sql);
        ps.setFetchSize(fetchSize);
        preparer.accept(ps);
        rs = ps.executeQuery();
        rs.setFetchSize(fetchSize);
      } catch (SQLException e) {
        try {
          close();
        } catch (Exception ex) {
          e.addSuppressed(ex);
        }
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      List<AutoCloseable> c = new ArrayList<>();
      c.add(rs);
      c.add(ps);
      c.add(conn);
      try {
        closeMultiple(c);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Nullable
    @Override
    protected R computeNext() {
      try {
        if (!rs.next()) {
          return endOfData();
        }

        return mapToObj(rs);
      } catch (SQLException e) {
        throw unhandledSQLException(e);
      }
    }

    protected abstract R mapToObj(ResultSet rs) throws SQLException;
  }

  protected RuntimeException unhandledSQLException(SQLException e) {
    return JdbcBackend.unhandledSQLException(databaseSpecific, e);
  }
}
