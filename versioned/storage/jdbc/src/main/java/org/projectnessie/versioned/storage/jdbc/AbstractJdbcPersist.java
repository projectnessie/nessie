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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.util.Closing.closeMultiple;
import static org.projectnessie.versioned.storage.jdbc.JdbcBackend.unhandledSQLException;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.ADD_REFERENCE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_CREATED;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_INCOMPLETE_INDEX;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_INCREMENTAL_INDEX;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_MESSAGE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_REFERENCE_INDEX;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_REFERENCE_INDEX_STRIPES;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_SECONDARY_PARENTS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_SEQ;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_TAIL;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_COMMIT_TYPE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_INDEX_INDEX;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REF_CREATED_AT;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REF_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REF_INITIAL_POINTER;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_REF_NAME;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_SEGMENTS_STRIPES;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_STRING_COMPRESSION;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_STRING_CONTENT_TYPE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_STRING_FILENAME;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_STRING_PREDECESSORS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_STRING_TEXT;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_TAG_HEADERS;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_TAG_MESSAGE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_TAG_SIGNATURE;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_VALUE_CONTENT_ID;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_VALUE_DATA;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.COL_VALUE_PAYLOAD;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.DELETE_OBJ;
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
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.STORE_OBJ;
import static org.projectnessie.versioned.storage.jdbc.SqlConstants.UPDATE_REFERENCE_POINTER;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.HeaderEntry;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Headers;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripe;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripes;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
abstract class AbstractJdbcPersist implements Persist {

  private final StoreConfig config;
  private final DatabaseSpecific databaseSpecific;

  AbstractJdbcPersist(DatabaseSpecific databaseSpecific, StoreConfig config) {
    this.config = config;
    this.databaseSpecific = databaseSpecific;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return JdbcBackendFactory.NAME;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public StoreConfig config() {
    return config;
  }

  protected final Reference findReference(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull String name) {
    return findReferences(conn, new String[] {name})[0];
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  protected final Reference[] findReferences(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull String[] names) {
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
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Reference ref = deserializeReference(rs);
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
  @jakarta.annotation.Nonnull
  protected final Reference addReference(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    String sql = databaseSpecific.wrapInsert(ADD_REFERENCE);
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, config.repositoryId());
      ps.setString(2, reference.name());
      serializeObjId(ps, 3, reference.pointer());
      ps.setBoolean(4, reference.deleted());
      if (reference.createdAtMicros() != 0L) {
        ps.setLong(5, reference.createdAtMicros());
      } else {
        ps.setNull(5, Types.BIGINT);
      }
      serializeObjId(ps, 6, reference.extendedInfoObj());
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
  @jakarta.annotation.Nonnull
  protected final Reference markReferenceAsDeleted(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try (PreparedStatement ps =
        conn.prepareStatement(referencesDml(MARK_REFERENCE_AS_DELETED, reference))) {
      int idx = 1;
      ps.setBoolean(idx++, true);
      ps.setString(idx++, config().repositoryId());
      ps.setString(idx++, reference.name());
      serializeObjId(ps, idx++, reference.pointer());
      ps.setBoolean(idx++, false);
      long createdAtMicros = reference.createdAtMicros();
      if (createdAtMicros != 0L) {
        ps.setLong(idx++, createdAtMicros);
      }
      ObjId extendedInfoObj = reference.extendedInfoObj();
      if (extendedInfoObj != null) {
        serializeObjId(ps, idx, extendedInfoObj);
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

  protected final void purgeReference(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try (PreparedStatement ps = conn.prepareStatement(referencesDml(PURGE_REFERENCE, reference))) {
      int idx = 1;
      ps.setString(idx++, config().repositoryId());
      ps.setString(idx++, reference.name());
      serializeObjId(ps, idx++, reference.pointer());
      ps.setBoolean(idx++, true);
      long createdAtMicros = reference.createdAtMicros();
      if (createdAtMicros != 0L) {
        ps.setLong(idx++, createdAtMicros);
      }
      ObjId extendedInfoObj = reference.extendedInfoObj();
      if (extendedInfoObj != null) {
        serializeObjId(ps, idx, extendedInfoObj);
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
  @jakarta.annotation.Nonnull
  protected final Reference updateReferencePointer(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    try (PreparedStatement ps =
        conn.prepareStatement(referencesDml(UPDATE_REFERENCE_POINTER, reference))) {
      int idx = 1;
      serializeObjId(ps, idx++, newPointer);
      Reference updated = reference.forNewPointer(newPointer, config);
      byte[] previous = serializePreviousPointers(updated.previousPointers());
      if (previous != null) {
        ps.setBytes(idx++, previous);
      } else {
        ps.setNull(idx++, Types.BINARY);
      }

      ps.setString(idx++, config().repositoryId());
      ps.setString(idx++, reference.name());
      serializeObjId(ps, idx++, reference.pointer());
      ps.setBoolean(idx++, false);
      long createdAtMicros = reference.createdAtMicros();
      if (createdAtMicros != 0L) {
        ps.setLong(idx++, createdAtMicros);
      }
      ObjId extendedInfoObj = reference.extendedInfoObj();
      if (extendedInfoObj != null) {
        serializeObjId(ps, idx, extendedInfoObj);
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

  @SuppressWarnings("unused")
  protected <T extends Obj> T fetchTypedObj(
      Connection conn, ObjId id, ObjType type, Class<T> typeClass) throws ObjNotFoundException {
    Obj obj = fetchObjs(conn, new ObjId[] {id}, type)[0];

    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  protected final Obj fetchObj(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    return fetchObjs(conn, new ObjId[] {id})[0];
  }

  protected ObjType fetchObjType(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    try (PreparedStatement ps = conn.prepareStatement(sqlSelectMultiple(FETCH_OBJ_TYPE, 1))) {
      ps.setString(1, config.repositoryId());
      serializeObjId(ps, 2, id);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          String objType = rs.getString(1);
          return ObjType.valueOf(objType);
        }
      }
      throw new ObjNotFoundException(id);
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  protected final Obj[] fetchObjs(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
      throws ObjNotFoundException {
    return fetchObjs(conn, ids, null);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  protected final Obj[] fetchObjs(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull ObjId[] ids,
      @Nullable @jakarta.annotation.Nullable ObjType type)
      throws ObjNotFoundException {
    Object2IntHashMap<ObjId> idToIndex =
        new Object2IntHashMap<>(200, Hashing.DEFAULT_LOAD_FACTOR, -1);
    Obj[] r = new Obj[ids.length];
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
        serializeObjId(ps, idx++, key);
      }
      if (type != null) {
        ps.setString(idx, type.name());
      }

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Obj obj = deserializeObj(rs);
          int i = idToIndex.getValue(obj.id());
          if (i != -1) {
            r[i] = obj;
          }
        }

        List<ObjId> notFound = null;
        for (int i = 0; i < ids.length; i++) {
          ObjId id = ids[i];
          if (r[i] == null && id != null) {
            if (notFound == null) {
              notFound = new ArrayList<>();
            }
            notFound.add(id);
          }
        }
        if (notFound != null) {
          throw new ObjNotFoundException(notFound);
        }

        return r;
      }
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  private Obj deserializeObj(ResultSet rs) throws SQLException {
    ObjId id = deserializeObjId(rs, 1);
    String objType = rs.getString(2);
    ObjType type = ObjType.valueOf(objType);

    @SuppressWarnings("rawtypes")
    StoreObjDesc objDesc = STORE_OBJ_TYPE.get(type);
    checkState(objDesc != null, "Cannot deserialize object type %s", objType);
    return objDesc.deserialize(rs, id);
  }

  protected final boolean storeObj(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Obj obj,
      boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return upsertObjs(conn, new Obj[] {obj}, ignoreSoftSizeRestrictions, true)[0];
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  protected final boolean[] storeObjs(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    return upsertObjs(conn, objs, false, true);
  }

  protected final Void updateObj(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Obj obj)
      throws ObjTooLargeException {
    updateObjs(conn, new Obj[] {obj});
    return null;
  }

  protected final Void updateObjs(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    upsertObjs(conn, objs, false, false);
    return null;
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  @jakarta.annotation.Nonnull
  private boolean[] upsertObjs(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull Obj[] objs,
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

    try (PreparedStatement ps = conn.prepareStatement(databaseSpecific.wrapInsert(STORE_OBJ))) {
      boolean[] r = new boolean[objs.length];

      Int2IntHashMap batchIndexToObjIndex =
          new Int2IntHashMap(objs.length * 2, Hashing.DEFAULT_LOAD_FACTOR, -1);

      Consumer<int[]> batchResultHandler =
          updated -> {
            for (int i = 0; i < updated.length; i++) {
              if (updated[i] == 1) {
                r[batchIndexToObjIndex.get(i)] = true;
              }
            }
          };

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

        checkArgument(STORE_OBJ_TYPE.containsKey(type), "Cannot serialize object type %s ", type);

        //

        int idx = 1;
        ps.setString(idx++, config.repositoryId());
        serializeObjId(ps, idx++, id);
        ps.setString(idx++, type.name());

        for (Entry<ObjType, StoreObjDesc<?>> e : STORE_OBJ_TYPE.entrySet()) {
          if (e.getKey() == type) {
            @SuppressWarnings("rawtypes")
            StoreObjDesc storeType = e.getValue();
            idx = storeType.store(ps, idx, obj, incrementalIndexSizeLimit, indexSizeLimit);
          } else {
            idx = e.getValue().storeNone(ps, idx);
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

      return r;
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

  protected final void deleteObj(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull ObjId id) {
    try (PreparedStatement ps = conn.prepareStatement(DELETE_OBJ)) {
      ps.setString(1, config.repositoryId());
      serializeObjId(ps, 2, id);

      ps.executeUpdate();
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  protected final void deleteObjs(
      @Nonnull @jakarta.annotation.Nonnull Connection conn,
      @Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
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
        serializeObjId(ps, 2, id);
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

    for (ObjId id : ids) {
      deleteObj(conn, id);
    }
  }

  protected CloseableIterator<Obj> scanAllObjects(Connection conn, Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(conn, returnedObjTypes);
  }

  private abstract static class StoreObjDesc<O extends Obj> {

    abstract O deserialize(ResultSet rs, ObjId id) throws SQLException;

    abstract int storeNone(PreparedStatement ps, int idx) throws SQLException;

    abstract int store(
        PreparedStatement ps, int idx, O obj, int incrementalIndexLimit, int maxSerializedIndexSize)
        throws SQLException, ObjTooLargeException;
  }

  private static final Map<ObjType, StoreObjDesc<?>> STORE_OBJ_TYPE = new EnumMap<>(ObjType.class);

  static {
    STORE_OBJ_TYPE.put(
        ObjType.COMMIT,
        new StoreObjDesc<CommitObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.BIGINT);
            ps.setNull(idx++, Types.BIGINT);
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.BINARY);
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.BINARY);

            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);

            ps.setNull(idx++, Types.BINARY);

            ps.setNull(idx++, Types.BOOLEAN);
            ps.setNull(idx++, Types.VARCHAR);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              CommitObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException, ObjTooLargeException {
            ps.setLong(idx++, obj.created());
            ps.setLong(idx++, obj.seq());
            ps.setString(idx++, obj.message());

            obj.headers();
            Headers.Builder hb = Headers.newBuilder();
            for (String h : obj.headers().keySet()) {
              hb.addHeaders(
                  HeaderEntry.newBuilder().setName(h).addAllValues(obj.headers().getAll(h)));
            }
            ps.setBytes(idx++, hb.build().toByteArray());

            serializeObjId(ps, idx++, obj.referenceIndex());

            Stripes.Builder b = Stripes.newBuilder();
            obj.referenceIndexStripes().stream()
                .map(
                    s ->
                        Stripe.newBuilder()
                            .setFirstKey(s.firstKey().rawString())
                            .setLastKey(s.lastKey().rawString())
                            .setSegment(s.segment().asBytes()))
                .forEach(b::addStripes);
            serializeBytes(ps, idx++, b.build().toByteString());

            serializeObjIds(ps, idx++, obj.tail());
            serializeObjIds(ps, idx++, obj.secondaryParents());

            ByteString index = obj.incrementalIndex();
            if (index.size() > incrementalIndexLimit) {
              throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
            }
            serializeBytes(ps, idx++, index);

            ps.setBoolean(idx++, obj.incompleteIndex());
            ps.setString(idx++, obj.commitType().name());
            return idx;
          }

          @Override
          CommitObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            CommitObj.Builder b =
                CommitObj.commitBuilder()
                    .id(id)
                    .created(rs.getLong(COL_COMMIT_CREATED))
                    .seq(rs.getLong(COL_COMMIT_SEQ))
                    .message(rs.getString(COL_COMMIT_MESSAGE))
                    .referenceIndex(deserializeObjId(rs, COL_COMMIT_REFERENCE_INDEX))
                    .incrementalIndex(deserializeBytes(rs, COL_COMMIT_INCREMENTAL_INDEX))
                    .incompleteIndex(rs.getBoolean(COL_COMMIT_INCOMPLETE_INDEX))
                    .commitType(CommitType.valueOf(rs.getString(COL_COMMIT_TYPE)));
            deserializeObjIds(rs, COL_COMMIT_TAIL, b::addTail);
            deserializeObjIds(rs, COL_COMMIT_SECONDARY_PARENTS, b::addSecondaryParents);

            try {
              CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
              Headers headers = Headers.parseFrom(rs.getBytes(COL_COMMIT_HEADERS));
              for (HeaderEntry e : headers.getHeadersList()) {
                for (String v : e.getValuesList()) {
                  h.add(e.getName(), v);
                }
              }
              b.headers(h.build());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            try {
              Stripes stripes = Stripes.parseFrom(rs.getBytes(COL_COMMIT_REFERENCE_INDEX_STRIPES));
              stripes.getStripesList().stream()
                  .map(
                      s ->
                          indexStripe(
                              keyFromString(s.getFirstKey()),
                              keyFromString(s.getLastKey()),
                              objIdFromByteBuffer(s.getSegment().asReadOnlyByteBuffer())))
                  .forEach(b::addReferenceIndexStripes);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            return b.build();
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.REF,
        new StoreObjDesc<RefObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.BIGINT);
            ps.setNull(idx++, Types.VARCHAR);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              RefObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException {
            ps.setString(idx++, obj.name());
            serializeObjId(ps, idx++, obj.initialPointer());
            ps.setLong(idx++, obj.createdAtMicros());
            serializeObjId(ps, idx++, obj.extendedInfoObj());
            return idx;
          }

          @Override
          RefObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            return ref(
                id,
                rs.getString(COL_REF_NAME),
                deserializeObjId(rs, COL_REF_INITIAL_POINTER),
                rs.getLong(COL_REF_CREATED_AT),
                deserializeObjId(rs, COL_REF_EXTENDED_INFO));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.VALUE,
        new StoreObjDesc<ContentValueObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.TINYINT);
            ps.setNull(idx++, Types.BINARY);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              ContentValueObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException {
            ps.setString(idx++, obj.contentId());
            ps.setInt(idx++, obj.payload());
            serializeBytes(ps, idx++, obj.data());
            return idx;
          }

          @Override
          ContentValueObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            return contentValue(
                id,
                rs.getString(COL_VALUE_CONTENT_ID),
                rs.getInt(COL_VALUE_PAYLOAD),
                requireNonNull(deserializeBytes(rs, COL_VALUE_DATA)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.INDEX_SEGMENTS,
        new StoreObjDesc<IndexSegmentsObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.BINARY);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              IndexSegmentsObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException {
            Stripes.Builder b = Stripes.newBuilder();
            obj.stripes().stream()
                .map(
                    s ->
                        Stripe.newBuilder()
                            .setFirstKey(s.firstKey().rawString())
                            .setLastKey(s.lastKey().rawString())
                            .setSegment(s.segment().asBytes()))
                .forEach(b::addStripes);
            serializeBytes(ps, idx++, b.build().toByteString());
            return idx;
          }

          @Override
          IndexSegmentsObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            try {
              Stripes stripes = Stripes.parseFrom(rs.getBytes(COL_SEGMENTS_STRIPES));
              List<IndexStripe> stripeList =
                  stripes.getStripesList().stream()
                      .map(
                          s ->
                              indexStripe(
                                  keyFromString(s.getFirstKey()),
                                  keyFromString(s.getLastKey()),
                                  objIdFromByteBuffer(s.getSegment().asReadOnlyByteBuffer())))
                      .collect(Collectors.toList());
              return indexSegments(id, stripeList);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.INDEX,
        new StoreObjDesc<IndexObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.BINARY);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              IndexObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException, ObjTooLargeException {
            ByteString index = obj.index();
            if (index.size() > maxSerializedIndexSize) {
              throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
            }
            serializeBytes(ps, idx++, index);
            return idx;
          }

          @Override
          IndexObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            ByteString index = deserializeBytes(rs, COL_INDEX_INDEX);
            if (index != null) {
              return index(id, index);
            }
            throw new IllegalStateException("Index column for object ID " + id + " is null");
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.TAG,
        new StoreObjDesc<TagObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.BINARY);
            ps.setNull(idx++, Types.BINARY);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              TagObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException {
            ps.setString(idx++, obj.message());
            Headers.Builder hb = Headers.newBuilder();
            CommitHeaders headers = obj.headers();
            if (headers != null) {
              for (String h : headers.keySet()) {
                hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(headers.getAll(h)));
              }
            }
            ps.setBytes(idx++, hb.build().toByteArray());
            serializeBytes(ps, idx++, obj.signature());
            return idx;
          }

          @Override
          TagObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            CommitHeaders tagHeaders = null;
            try {
              Headers headers = Headers.parseFrom(deserializeBytes(rs, COL_TAG_HEADERS));
              if (headers.getHeadersCount() > 0) {
                CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
                for (HeaderEntry e : headers.getHeadersList()) {
                  for (String v : e.getValuesList()) {
                    h.add(e.getName(), v);
                  }
                }
                tagHeaders = h.build();
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            return tag(
                id,
                rs.getString(COL_TAG_MESSAGE),
                tagHeaders,
                deserializeBytes(rs, COL_TAG_SIGNATURE));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.STRING,
        new StoreObjDesc<StringObj>() {
          @Override
          int storeNone(PreparedStatement ps, int idx) throws SQLException {
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.VARCHAR);
            ps.setNull(idx++, Types.BINARY);
            return idx;
          }

          @Override
          int store(
              PreparedStatement ps,
              int idx,
              StringObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws SQLException {
            ps.setString(idx++, obj.contentType());
            ps.setString(idx++, obj.compression().name());
            ps.setString(idx++, obj.filename());
            serializeObjIds(ps, idx++, obj.predecessors());
            serializeBytes(ps, idx++, obj.text());
            return idx;
          }

          @Override
          StringObj deserialize(ResultSet rs, ObjId id) throws SQLException {
            return stringData(
                id,
                rs.getString(COL_STRING_CONTENT_TYPE),
                Compression.valueOf(rs.getString(COL_STRING_COMPRESSION)),
                rs.getString(COL_STRING_FILENAME),
                deserializeObjIds(rs, COL_STRING_PREDECESSORS),
                deserializeBytes(rs, COL_STRING_TEXT));
          }
        });
  }

  private static void serializeBytes(PreparedStatement ps, int idx, ByteString blob)
      throws SQLException {
    if (blob == null) {
      ps.setNull(idx, Types.BLOB);
      return;
    }
    ps.setBinaryStream(idx, blob.newInput());
  }

  private static ByteString deserializeBytes(ResultSet rs, int idx) throws SQLException {
    byte[] bytes = rs.getBytes(idx);
    return bytes != null ? unsafeWrap(bytes) : null;
  }

  private static Reference deserializeReference(ResultSet rs) throws SQLException {
    byte[] prevBytes = rs.getBytes(6);
    List<Reference.PreviousPointer> previousPointers =
        prevBytes != null ? deserializePreviousPointers(prevBytes) : emptyList();
    return Reference.reference(
        rs.getString(1),
        deserializeObjId(rs, 2),
        rs.getBoolean(3),
        rs.getLong(4),
        deserializeObjId(rs, 5),
        previousPointers);
  }

  private static ObjId deserializeObjId(ResultSet rs, int col) throws SQLException {
    String s = rs.getString(col);
    return s != null ? objIdFromString(s) : null;
  }

  private static void serializeObjId(PreparedStatement ps, int col, ObjId value)
      throws SQLException {
    if (value != null) {
      ps.setString(col, value.toString());
    } else {
      ps.setNull(col, Types.VARCHAR);
    }
  }

  @SuppressWarnings("SameParameterValue")
  private static List<ObjId> deserializeObjIds(ResultSet rs, int col) throws SQLException {
    List<ObjId> r = new ArrayList<>();
    deserializeObjIds(rs, col, r::add);
    return r;
  }

  private static void deserializeObjIds(ResultSet rs, int col, Consumer<ObjId> consumer)
      throws SQLException {
    String s = rs.getString(col);
    if (s == null || s.isEmpty()) {
      return;
    }
    int i = 0;
    while (true) {
      int next = s.indexOf(',', i);
      String idAsString;
      if (next == -1) {
        idAsString = s.substring(i);
      } else {
        idAsString = s.substring(i, next);
        i = next + 1;
      }
      consumer.accept(objIdFromString(idAsString));
      if (next == -1) {
        return;
      }
    }
  }

  private static void serializeObjIds(PreparedStatement ps, int col, List<ObjId> values)
      throws SQLException {
    if (values != null && !values.isEmpty()) {
      ps.setString(col, values.stream().map(ObjId::toString).collect(Collectors.joining(",")));
    } else {
      ps.setNull(col, Types.VARCHAR);
    }
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

  private class ScanAllObjectsIterator extends ResultSetIterator<Obj> {
    ScanAllObjectsIterator(Connection conn, Set<ObjType> returnedObjTypes) {
      super(
          conn,
          sqlSelectMultiple(SCAN_OBJS, returnedObjTypes.size()),
          ps -> {
            int idx = 1;
            ps.setString(idx++, config.repositoryId());
            for (ObjType returnedObjType : returnedObjTypes) {
              ps.setString(idx++, returnedObjType.name());
            }
          });
    }

    @Override
    protected Obj mapToObj(ResultSet rs) throws SQLException {
      return deserializeObj(rs);
    }
  }

  private abstract static class ResultSetIterator<R> extends AbstractIterator<R>
      implements CloseableIterator<R> {

    private final Connection conn;
    private final PreparedStatement ps;
    private final ResultSet rs;

    ResultSetIterator(Connection conn, String sql, ThrowingConsumer<PreparedStatement> preparer) {
      this.conn = conn;

      try {
        ps = conn.prepareStatement(sql);
        preparer.accept(ps);
        rs = ps.executeQuery();
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
    @jakarta.annotation.Nullable
    @Override
    protected R computeNext() {
      try {
        if (!rs.next()) {
          return endOfData();
        }

        return mapToObj(rs);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    protected abstract R mapToObj(ResultSet rs) throws SQLException;
  }
}
