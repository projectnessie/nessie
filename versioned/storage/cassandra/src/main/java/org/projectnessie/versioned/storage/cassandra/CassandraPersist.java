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
package org.projectnessie.versioned.storage.cassandra;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.ADD_REFERENCE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_CREATED;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_INCOMPLETE_INDEX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_INCREMENTAL_INDEX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_MESSAGE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_REFERENCE_INDEX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_REFERENCE_INDEX_STRIPES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_SECONDARY_PARENTS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_SEQ;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_TAIL;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_COMMIT_TYPE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_INDEX_INDEX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REF_CREATED_AT;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REF_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REF_INITIAL_POINTER;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REF_NAME;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_SEGMENTS_STRIPES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_STRING_COMPRESSION;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_STRING_CONTENT_TYPE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_STRING_FILENAME;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_STRING_PREDECESSORS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_STRING_TEXT;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_TAG_HEADERS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_TAG_MESSAGE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_TAG_SIGNATURE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_VALUE_CONTENT_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_VALUE_DATA;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_VALUE_PAYLOAD;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.DELETE_OBJ;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.FETCH_OBJ_TYPE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.FIND_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.FIND_REFERENCES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_COMMIT;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_INDEX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_REF;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_SEGMENTS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_STRING;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_TAG;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_VALUE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.MARK_REFERENCE_AS_DELETED;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.MAX_CONCURRENT_STORES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.PURGE_REFERENCE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.SCAN_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.UPDATE_REFERENCE_POINTER;
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
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.cassandra.CassandraBackend.BatchedQuery;
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

public class CassandraPersist implements Persist {

  private final CassandraBackend backend;
  private final StoreConfig config;

  CassandraPersist(CassandraBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return CassandraBackendFactory.NAME;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public StoreConfig config() {
    return config;
  }

  @Override
  public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
    return fetchReferences(new String[] {name})[0];
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
    try (BatchedQuery<String, Reference> batchedQuery =
        backend.newBatchedQuery(
            keys -> backend.executeAsync(FIND_REFERENCES, config.repositoryId(), keys),
            CassandraPersist::deserializeReference,
            Reference::name,
            names.length,
            Reference.class)) {

      for (int i = 0; i < names.length; i++) {
        String name = names[i];
        if (name != null) {
          batchedQuery.add(name, i);
        }
      }

      return batchedQuery.finish();
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    byte[] serializedPreviousPointers = serializePreviousPointers(reference.previousPointers());
    ByteBuffer previous =
        serializedPreviousPointers != null ? ByteBuffer.wrap(serializedPreviousPointers) : null;
    if (backend.executeCas(
        ADD_REFERENCE,
        config.repositoryId(),
        reference.name(),
        serializeObjId(reference.pointer()),
        reference.deleted(),
        reference.createdAtMicros(),
        serializeObjId(reference.extendedInfoObj()),
        previous)) {
      return reference;
    }
    throw new RefAlreadyExistsException(fetchReference(reference.name()));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    if (backend.executeCas(
        MARK_REFERENCE_AS_DELETED,
        true,
        config().repositoryId(),
        reference.name(),
        serializeObjId(reference.pointer()),
        false,
        reference.createdAtMicros(),
        serializeObjId(reference.extendedInfoObj()))) {
      return reference.withDeleted(true);
    }

    Reference ref = fetchReference(reference.name());
    if (ref == null) {
      throw new RefNotFoundException(reference);
    }
    throw new RefConditionFailedException(ref);
  }

  @Override
  public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    if (!backend.executeCas(
        PURGE_REFERENCE,
        config().repositoryId(),
        reference.name(),
        serializeObjId(reference.pointer()),
        true,
        reference.createdAtMicros(),
        serializeObjId(reference.extendedInfoObj()))) {
      Reference ref = fetchReference(reference.name());
      if (ref == null) {
        throw new RefNotFoundException(reference);
      }
      throw new RefConditionFailedException(ref);
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference updateReferencePointer(
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    Reference updated = reference.forNewPointer(newPointer, config);
    byte[] serializedPreviousPointers = serializePreviousPointers(updated.previousPointers());
    ByteBuffer previous =
        serializedPreviousPointers != null ? ByteBuffer.wrap(serializedPreviousPointers) : null;
    if (!backend.executeCas(
        UPDATE_REFERENCE_POINTER,
        serializeObjId(newPointer),
        previous,
        config().repositoryId(),
        reference.name(),
        serializeObjId(reference.pointer()),
        false,
        reference.createdAtMicros(),
        serializeObjId(reference.extendedInfoObj()))) {
      Reference ref = fetchReference(reference.name());
      if (ref == null) {
        throw new RefNotFoundException(reference);
      }
      throw new RefConditionFailedException(ref);
    }

    return updated;
  }

  @SuppressWarnings("unused")
  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull @jakarta.annotation.Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    Obj obj = fetchObjs(new ObjId[] {id}, type)[0];

    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Obj fetchObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) throws ObjNotFoundException {
    return fetchObjs(new ObjId[] {id})[0];
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    Row row =
        backend
            .execute(FETCH_OBJ_TYPE, config.repositoryId(), singletonList(serializeObjId(id)))
            .one();
    if (row != null) {
      String objType = row.getString(0);
      return ObjType.valueOf(objType);
    }
    throw new ObjNotFoundException(id);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
      throws ObjNotFoundException {
    return fetchObjs(ids, null);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  Obj[] fetchObjs(
      @Nonnull @jakarta.annotation.Nonnull ObjId[] ids,
      @Nullable @jakarta.annotation.Nullable ObjType type)
      throws ObjNotFoundException {
    Function<List<ObjId>, List<String>> idsToStrings =
        queryIds -> queryIds.stream().map(ObjId::toString).collect(Collectors.toList());

    Function<List<ObjId>, CompletionStage<AsyncResultSet>> queryFunc =
        keys -> backend.executeAsync(FIND_OBJS, config.repositoryId(), idsToStrings.apply(keys));

    Function<Row, Obj> rowMapper =
        row -> {
          ObjType objType = ObjType.valueOf(row.getString(1));
          return deserializeObj(row, objType);
        };

    Obj[] r;
    try (BatchedQuery<ObjId, Obj> batchedQuery =
        backend.newBatchedQuery(queryFunc, rowMapper, Obj::id, ids.length, Obj.class)) {

      for (int i = 0; i < ids.length; i++) {
        ObjId id = ids[i];
        if (id != null) {
          batchedQuery.add(id, i);
        }
      }

      r = batchedQuery.finish();
    }

    List<ObjId> notFound = null;
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null && (r[i] == null || (type != null && r[i].type() != type))) {
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

  private Obj deserializeObj(Row row, ObjType type) {
    ObjId id = deserializeObjId(row.getString(0));

    @SuppressWarnings("rawtypes")
    StoreObjDesc objDesc = STORE_OBJ_TYPE.get(type);
    checkState(objDesc != null, "Cannot deserialize object type %s", type);
    return objDesc.deserialize(row, id);
  }

  @Override
  public void upsertObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    writeSingleObj(
        obj,
        ignoreSoftSizeRestrictions,
        (storeObj, values) -> backend.executeCas(storeObj.cql(), values));
  }

  @Override
  public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    AtomicIntegerArray results = new AtomicIntegerArray(objs.length);

    try (LimitedConcurrentRequests requests =
        new LimitedConcurrentRequests(MAX_CONCURRENT_STORES)) {
      for (int i = 0; i < objs.length; i++) {
        Obj o = objs[i];
        if (o != null) {
          int idx = i;
          writeSingleObj(
              o,
              false,
              (storeObj, values) -> {
                CompletionStage<?> cs =
                    backend
                        .executeAsync(storeObj.cql(), values)
                        .handle(
                            (resultSet, e) -> {
                              if (e != null) {
                                if (e instanceof DriverException) {
                                  backend.handleDriverException((DriverException) e);
                                }
                                if (e instanceof RuntimeException) {
                                  throw (RuntimeException) e;
                                }
                                throw new RuntimeException(e);
                              }

                              if (resultSet.wasApplied()) {
                                results.set(idx, 1);
                              }
                              return null;
                            })
                        .thenAccept(x -> {});
                requests.submitted(cs);
              });
        }
      }
    }
  }

  @FunctionalInterface
  private interface WriteSingleObj {
    void apply(StoreObjDesc<?> storeObj, Object[] values);
  }

  private void writeSingleObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj,
      boolean ignoreSoftSizeRestrictions,
      WriteSingleObj consumer)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    ObjType type = obj.type();

    StoreObjDesc<Obj> storeObj = storeObjForObj(type);

    List<Object> values = new ArrayList<>();
    values.add(config.repositoryId());
    values.add(serializeObjId(id));
    values.add(type.name());
    storeObj.store(
        values::add,
        obj,
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit(),
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit());

    consumer.apply(storeObj, values.toArray(new Object[0]));
  }

  @SuppressWarnings("unchecked")
  private static StoreObjDesc<Obj> storeObjForObj(ObjType type) {
    @SuppressWarnings("rawtypes")
    StoreObjDesc storeObj = STORE_OBJ_TYPE.get(type);
    checkArgument(storeObj != null, "Cannot serialize object type %s ", type);
    return storeObj;
  }

  @Override
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    backend.execute(DELETE_OBJ, config.repositoryId(), serializeObjId(id));
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    try (LimitedConcurrentRequests requests =
        new LimitedConcurrentRequests(MAX_CONCURRENT_STORES)) {
      String repoId = config.repositoryId();
      for (ObjId id : ids) {
        if (id != null) {
          requests.submitted(backend.executeAsync(DELETE_OBJ, repoId, serializeObjId(id)));
        }
      }
    }
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public CloseableIterator<Obj> scanAllObjects(
      @Nonnull @jakarta.annotation.Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes);
  }

  private abstract static class StoreObjDesc<O extends Obj> {
    private final String insertCql;

    StoreObjDesc(String insertCql) {
      this.insertCql = insertCql;
    }

    abstract O deserialize(Row row, ObjId id);

    abstract void store(
        Consumer<Object> values, O obj, int incrementalIndexLimit, int maxSerializedIndexSize)
        throws ObjTooLargeException;

    String cql() {
      return insertCql;
    }
  }

  private static final Map<ObjType, StoreObjDesc<?>> STORE_OBJ_TYPE = new EnumMap<>(ObjType.class);

  static {
    STORE_OBJ_TYPE.put(
        ObjType.COMMIT,
        new StoreObjDesc<CommitObj>(INSERT_OBJ_COMMIT) {
          @Override
          void store(
              Consumer<Object> values,
              CommitObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws ObjTooLargeException {
            values.accept(obj.created());
            values.accept(obj.seq());
            values.accept(obj.message());

            Headers.Builder hb = Headers.newBuilder();
            for (String h : obj.headers().keySet()) {
              hb.addHeaders(
                  HeaderEntry.newBuilder().setName(h).addAllValues(obj.headers().getAll(h)));
            }
            values.accept(ByteBuffer.wrap(hb.build().toByteArray()));

            values.accept(serializeObjId(obj.referenceIndex()));

            Stripes.Builder b = Stripes.newBuilder();
            obj.referenceIndexStripes().stream()
                .map(
                    s ->
                        Stripe.newBuilder()
                            .setFirstKey(s.firstKey().rawString())
                            .setLastKey(s.lastKey().rawString())
                            .setSegment(s.segment().asBytes()))
                .forEach(b::addStripes);
            values.accept(b.build().toByteString().asReadOnlyByteBuffer());

            values.accept(serializeObjIds(obj.tail()));
            values.accept(serializeObjIds(obj.secondaryParents()));

            ByteString index = obj.incrementalIndex();
            if (index.size() > incrementalIndexLimit) {
              throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
            }
            values.accept(index.asReadOnlyByteBuffer());

            values.accept(obj.incompleteIndex());
            values.accept(obj.commitType().name());
          }

          @Override
          CommitObj deserialize(Row row, ObjId id) {
            CommitObj.Builder b =
                CommitObj.commitBuilder()
                    .id(id)
                    .created(row.getLong(COL_COMMIT_CREATED))
                    .seq(row.getLong(COL_COMMIT_SEQ))
                    .message(row.getString(COL_COMMIT_MESSAGE))
                    .referenceIndex(deserializeObjId(row.getString(COL_COMMIT_REFERENCE_INDEX)))
                    .incrementalIndex(deserializeBytes(row, COL_COMMIT_INCREMENTAL_INDEX))
                    .incompleteIndex(row.getBoolean(COL_COMMIT_INCOMPLETE_INDEX))
                    .commitType(CommitType.valueOf(row.getString(COL_COMMIT_TYPE)));
            deserializeObjIds(row, COL_COMMIT_TAIL, b::addTail);
            deserializeObjIds(row, COL_COMMIT_SECONDARY_PARENTS, b::addSecondaryParents);

            try {
              CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
              Headers headers = Headers.parseFrom(row.getByteBuffer(COL_COMMIT_HEADERS));
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
              Stripes stripes =
                  Stripes.parseFrom(row.getByteBuffer(COL_COMMIT_REFERENCE_INDEX_STRIPES));
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
        new StoreObjDesc<RefObj>(INSERT_OBJ_REF) {
          @Override
          void store(
              Consumer<Object> values,
              RefObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            values.accept(obj.name());
            values.accept(serializeObjId(obj.initialPointer()));
            values.accept(obj.createdAtMicros());
            values.accept(serializeObjId(obj.extendedInfoObj()));
          }

          @Override
          RefObj deserialize(Row row, ObjId id) {
            return ref(
                id,
                row.getString(COL_REF_NAME),
                deserializeObjId(row.getString(COL_REF_INITIAL_POINTER)),
                row.getLong(COL_REF_CREATED_AT),
                deserializeObjId(row.getString(COL_REF_EXTENDED_INFO)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.VALUE,
        new StoreObjDesc<ContentValueObj>(INSERT_OBJ_VALUE) {
          @Override
          void store(
              Consumer<Object> values,
              ContentValueObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            values.accept(obj.contentId());
            values.accept(obj.payload());
            values.accept(obj.data().asReadOnlyByteBuffer());
          }

          @Override
          ContentValueObj deserialize(Row row, ObjId id) {
            ByteString value = deserializeBytes(row, COL_VALUE_DATA);
            if (value != null) {
              return contentValue(
                  id, row.getString(COL_VALUE_CONTENT_ID), row.getInt(COL_VALUE_PAYLOAD), value);
            }
            throw new IllegalStateException("Data value of obj " + id + " of type VALUE is null");
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.INDEX_SEGMENTS,
        new StoreObjDesc<IndexSegmentsObj>(INSERT_OBJ_SEGMENTS) {
          @Override
          void store(
              Consumer<Object> values,
              IndexSegmentsObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            Stripes.Builder b = Stripes.newBuilder();
            obj.stripes().stream()
                .map(
                    s ->
                        Stripe.newBuilder()
                            .setFirstKey(s.firstKey().rawString())
                            .setLastKey(s.lastKey().rawString())
                            .setSegment(s.segment().asBytes()))
                .forEach(b::addStripes);
            values.accept(b.build().toByteString().asReadOnlyByteBuffer());
          }

          @Override
          IndexSegmentsObj deserialize(Row row, ObjId id) {
            try {
              Stripes stripes = Stripes.parseFrom(row.getByteBuffer(COL_SEGMENTS_STRIPES));
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
        new StoreObjDesc<IndexObj>(INSERT_OBJ_INDEX) {
          @Override
          void store(
              Consumer<Object> values,
              IndexObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize)
              throws ObjTooLargeException {
            ByteString index = obj.index();
            if (index.size() > maxSerializedIndexSize) {
              throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
            }
            values.accept(index.asReadOnlyByteBuffer());
          }

          @Override
          IndexObj deserialize(Row row, ObjId id) {
            ByteString indexValue = deserializeBytes(row, COL_INDEX_INDEX);
            if (indexValue != null) {
              return index(id, indexValue);
            }
            throw new IllegalStateException("Index value of obj " + id + " of type INDEX is null");
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.TAG,
        new StoreObjDesc<TagObj>(INSERT_OBJ_TAG) {
          @Override
          void store(
              Consumer<Object> values,
              TagObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            values.accept(obj.message());
            Headers.Builder hb = Headers.newBuilder();
            CommitHeaders headers = obj.headers();
            if (headers != null) {
              for (String h : headers.keySet()) {
                hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(headers.getAll(h)));
              }
            }
            values.accept(ByteBuffer.wrap(hb.build().toByteArray()));
            ByteString signature = obj.signature();
            values.accept(signature != null ? signature.asReadOnlyByteBuffer() : null);
          }

          @Override
          TagObj deserialize(Row row, ObjId id) {
            CommitHeaders tagHeaders = null;
            try {
              Headers headers = Headers.parseFrom(row.getByteBuffer(COL_TAG_HEADERS));
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
                row.getString(COL_TAG_MESSAGE),
                tagHeaders,
                deserializeBytes(row, COL_TAG_SIGNATURE));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.STRING,
        new StoreObjDesc<StringObj>(INSERT_OBJ_STRING) {
          @Override
          void store(
              Consumer<Object> values,
              StringObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            values.accept(obj.contentType());
            values.accept(obj.compression().name());
            values.accept(obj.filename());
            values.accept(serializeObjIds(obj.predecessors()));
            values.accept(obj.text().asReadOnlyByteBuffer());
          }

          @Override
          StringObj deserialize(Row row, ObjId id) {
            return stringData(
                id,
                row.getString(COL_STRING_CONTENT_TYPE),
                Compression.valueOf(row.getString(COL_STRING_COMPRESSION)),
                row.getString(COL_STRING_FILENAME),
                deserializeObjIds(row, COL_STRING_PREDECESSORS),
                deserializeBytes(row, COL_STRING_TEXT));
          }
        });
  }

  private static ByteString deserializeBytes(Row row, int idx) {
    ByteBuffer bytes = row.getByteBuffer(idx);
    return bytes != null ? unsafeWrap(bytes) : null;
  }

  private static Reference deserializeReference(Row row) {
    ByteBuffer previous = row.getByteBuffer(5);
    byte[] bytes;
    if (previous != null) {
      bytes = new byte[previous.remaining()];
      previous.get(bytes);
    } else {
      bytes = null;
    }
    return Reference.reference(
        row.getString(0),
        deserializeObjId(row.getString(1)),
        row.getBoolean(2),
        row.getLong(3),
        deserializeObjId(row.getString(4)),
        deserializePreviousPointers(bytes));
  }

  private static ObjId deserializeObjId(String id) {
    return id != null ? objIdFromString(id) : null;
  }

  private static String serializeObjId(ObjId id) {
    return id != null ? id.toString() : null;
  }

  @SuppressWarnings("SameParameterValue")
  private static List<ObjId> deserializeObjIds(Row row, int col) {
    List<ObjId> r = new ArrayList<>();
    deserializeObjIds(row, col, r::add);
    return r;
  }

  private static void deserializeObjIds(Row row, int col, Consumer<ObjId> consumer) {
    List<String> s = row.getList(col, String.class);
    if (s == null || s.isEmpty()) {
      return;
    }
    s.stream().map(ObjId::objIdFromString).forEach(consumer);
  }

  private static List<String> serializeObjIds(List<ObjId> values) {
    return (values != null && !values.isEmpty())
        ? values.stream().map(ObjId::toString).collect(Collectors.toList())
        : null;
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Iterator<Row> rs;
    private final Set<ObjType> returnedObjTypes;

    ScanAllObjectsIterator(Set<ObjType> returnedObjTypes) {
      this.returnedObjTypes = returnedObjTypes;
      rs = backend.execute(SCAN_OBJS, config.repositoryId()).iterator();
    }

    @Override
    public void close() {}

    @Nullable
    @jakarta.annotation.Nullable
    @Override
    protected Obj computeNext() {
      while (true) {
        if (!rs.hasNext()) {
          return endOfData();
        }

        Row row = rs.next();
        ObjType type = ObjType.valueOf(row.getString(1));
        if (!returnedObjTypes.contains(type)) {
          continue;
        }

        return deserializeObj(row, type);
      }
    }
  }
}
