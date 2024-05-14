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
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.ADD_REFERENCE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_OBJ_VERS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.DELETE_OBJ;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.DELETE_OBJ_CONDITIONAL;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.EXPECTED_SUFFIX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.FETCH_OBJ_TYPE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.FIND_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.FIND_REFERENCES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.MARK_REFERENCE_AS_DELETED;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.MAX_CONCURRENT_STORES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.PURGE_REFERENCE;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.SCAN_OBJS;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.UPDATE_REFERENCE_POINTER;
import static org.projectnessie.versioned.storage.cassandra.CassandraSerde.deserializeObjId;
import static org.projectnessie.versioned.storage.cassandra.CassandraSerde.serializeObjId;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.cassandra.CassandraBackend.BatchedQuery;
import org.projectnessie.versioned.storage.cassandra.serializers.ObjSerializer;
import org.projectnessie.versioned.storage.cassandra.serializers.ObjSerializers;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;

public class CassandraPersist implements Persist {

  private final CassandraBackend backend;
  private final StoreConfig config;

  CassandraPersist(CassandraBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
  }

  @Nonnull
  @Override
  public String name() {
    return CassandraBackendFactory.NAME;
  }

  @Nonnull
  @Override
  public StoreConfig config() {
    return config;
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    return fetchReferences(new String[] {name})[0];
  }

  @Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull String[] names) {
    try (BatchedQuery<String, Reference> batchedQuery =
        backend.newBatchedQuery(
            keys ->
                backend.executeAsync(
                    backend.buildStatement(FIND_REFERENCES, config.repositoryId(), keys)),
            CassandraSerde::deserializeReference,
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
  @Override
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    byte[] serializedPreviousPointers = serializePreviousPointers(reference.previousPointers());
    ByteBuffer previous =
        serializedPreviousPointers != null ? ByteBuffer.wrap(serializedPreviousPointers) : null;
    BoundStatement stmt =
        backend.buildStatement(
            ADD_REFERENCE,
            config.repositoryId(),
            reference.name(),
            serializeObjId(reference.pointer()),
            reference.deleted(),
            reference.createdAtMicros(),
            serializeObjId(reference.extendedInfoObj()),
            previous);
    if (backend.executeCas(stmt)) {
      return reference;
    }
    throw new RefAlreadyExistsException(fetchReference(reference.name()));
  }

  @Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    BoundStatement stmt =
        backend.buildStatement(
            MARK_REFERENCE_AS_DELETED,
            true,
            config().repositoryId(),
            reference.name(),
            serializeObjId(reference.pointer()),
            false,
            reference.createdAtMicros(),
            serializeObjId(reference.extendedInfoObj()));
    if (backend.executeCas(stmt)) {
      return reference.withDeleted(true);
    }

    Reference ref = fetchReference(reference.name());
    if (ref == null) {
      throw new RefNotFoundException(reference);
    }
    throw new RefConditionFailedException(ref);
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    BoundStatement stmt =
        backend.buildStatement(
            PURGE_REFERENCE,
            config().repositoryId(),
            reference.name(),
            serializeObjId(reference.pointer()),
            true,
            reference.createdAtMicros(),
            serializeObjId(reference.extendedInfoObj()));
    if (!backend.executeCas(stmt)) {
      Reference ref = fetchReference(reference.name());
      if (ref == null) {
        throw new RefNotFoundException(reference);
      }
      throw new RefConditionFailedException(ref);
    }
  }

  @Nonnull
  @Override
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    Reference updated = reference.forNewPointer(newPointer, config);
    byte[] serializedPreviousPointers = serializePreviousPointers(updated.previousPointers());
    ByteBuffer previous =
        serializedPreviousPointers != null ? ByteBuffer.wrap(serializedPreviousPointers) : null;
    BoundStatement stmt =
        backend.buildStatement(
            UPDATE_REFERENCE_POINTER,
            serializeObjId(newPointer),
            previous,
            config().repositoryId(),
            reference.name(),
            serializeObjId(reference.pointer()),
            false,
            reference.createdAtMicros(),
            serializeObjId(reference.extendedInfoObj()));
    if (!backend.executeCas(stmt)) {
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
  public <T extends Obj> T fetchTypedObj(@Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    Obj obj = fetchObjs(new ObjId[] {id}, type)[0];

    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
    return fetchObjs(new ObjId[] {id})[0];
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    BoundStatement stmt =
        backend.buildStatement(
            FETCH_OBJ_TYPE, config.repositoryId(), singletonList(serializeObjId(id)));
    Row row = backend.execute(stmt).one();
    if (row != null) {
      String objType = requireNonNull(row.getString(0));
      return ObjTypes.forName(objType);
    }
    throw new ObjNotFoundException(id);
  }

  @Nonnull
  @Override
  public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
    return fetchObjs(ids, null);
  }

  @Nonnull
  Obj[] fetchObjs(@Nonnull ObjId[] ids, @Nullable ObjType type) throws ObjNotFoundException {
    Function<List<ObjId>, List<String>> idsToStrings =
        queryIds -> queryIds.stream().map(ObjId::toString).collect(Collectors.toList());

    Function<List<ObjId>, CompletionStage<AsyncResultSet>> queryFunc =
        keys ->
            backend.executeAsync(
                backend.buildStatement(FIND_OBJS, config.repositoryId(), idsToStrings.apply(keys)));

    Function<Row, Obj> rowMapper =
        row -> {
          ObjType objType = ObjTypes.forName(requireNonNull(row.getString(COL_OBJ_TYPE.name())));
          ObjId id = deserializeObjId(row.getString(COL_OBJ_ID.name()));
          String versionToken = row.getString(COL_OBJ_VERS.name());
          return ObjSerializers.forType(objType).deserialize(row, objType, id, versionToken);
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
      if (id != null && (r[i] == null || (type != null && !r[i].type().equals(type)))) {
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

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return writeSingleObj(obj, false, ignoreSoftSizeRestrictions, backend::executeCas);
  }

  @Nonnull
  @Override
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    return persistObjs(objs, false);
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    writeSingleObj(obj, true, false, backend::execute);
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    persistObjs(objs, true);
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    BoundStatement stmt =
        backend.buildStatement(
            DELETE_OBJ_CONDITIONAL,
            config.repositoryId(),
            serializeObjId(obj.id()),
            obj.type().name(),
            obj.versionToken());
    return backend.executeCas(stmt);
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    ObjId id = expected.id();
    ObjType type = expected.type();
    String expectedVersion = expected.versionToken();
    String newVersion = newValue.versionToken();

    checkArgument(id != null && id.equals(newValue.id()));
    checkArgument(type.equals(newValue.type()));
    checkArgument(!expectedVersion.equals(newVersion));

    ObjSerializer<Obj> serializer = ObjSerializers.forType(type);

    BoundStatementBuilder stmt =
        backend
            .newBoundStatementBuilder(serializer.updateConditionalCql())
            .setString(COL_REPO_ID.name(), config.repositoryId())
            .setString(COL_OBJ_ID.name(), serializeObjId(id))
            .setString(COL_OBJ_TYPE.name() + EXPECTED_SUFFIX, type.name())
            .setString(COL_OBJ_VERS.name() + EXPECTED_SUFFIX, expectedVersion)
            .setString(COL_OBJ_VERS.name(), newVersion);

    serializer.serialize(
        newValue, stmt, effectiveIncrementalIndexSizeLimit(), effectiveIndexSegmentSizeLimit());

    return backend.executeCas(stmt.build());
  }

  @Nonnull
  private boolean[] persistObjs(@Nonnull Obj[] objs, boolean upsert) throws ObjTooLargeException {
    AtomicIntegerArray results = new AtomicIntegerArray(objs.length);

    try (LimitedConcurrentRequests requests =
        new LimitedConcurrentRequests(MAX_CONCURRENT_STORES)) {
      for (int i = 0; i < objs.length; i++) {
        Obj o = objs[i];
        if (o != null) {
          int idx = i;
          CompletionStage<?> cs =
              writeSingleObj(o, upsert, false, backend::executeAsync)
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
                      });
          requests.submitted(cs);
        }
      }
    }

    int l = results.length();
    boolean[] array = new boolean[l];
    for (int i = 0; i < l; i++) {
      array[i] = results.get(i) == 1;
    }
    return array;
  }

  @FunctionalInterface
  private interface WriteSingleObj<R> {
    R apply(BoundStatement stmt);
  }

  private <R> R writeSingleObj(
      @Nonnull Obj obj,
      boolean upsert,
      boolean ignoreSoftSizeRestrictions,
      WriteSingleObj<R> consumer)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    ObjType type = obj.type();
    String versionToken =
        (obj instanceof UpdateableObj) ? ((UpdateableObj) obj).versionToken() : null;

    ObjSerializer<Obj> serializer = ObjSerializers.forType(type);

    BoundStatementBuilder stmt =
        backend
            .newBoundStatementBuilder(serializer.insertCql(upsert))
            .setString(COL_REPO_ID.name(), config.repositoryId())
            .setString(COL_OBJ_ID.name(), serializeObjId(id))
            .setString(COL_OBJ_TYPE.name(), type.name())
            .setString(COL_OBJ_VERS.name(), versionToken);

    serializer.serialize(
        obj,
        stmt,
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit(),
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit());

    return consumer.apply(stmt.build());
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    BoundStatement stmt =
        backend.buildStatement(DELETE_OBJ, config.repositoryId(), serializeObjId(id));
    backend.execute(stmt);
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    try (LimitedConcurrentRequests requests =
        new LimitedConcurrentRequests(MAX_CONCURRENT_STORES)) {
      String repoId = config.repositoryId();
      for (ObjId id : ids) {
        if (id != null) {
          BoundStatement stmt = backend.buildStatement(DELETE_OBJ, repoId, serializeObjId(id));
          requests.submitted(backend.executeAsync(stmt));
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
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes);
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Iterator<Row> rs;
    private final Set<ObjType> returnedObjTypes;

    ScanAllObjectsIterator(Set<ObjType> returnedObjTypes) {
      this.returnedObjTypes = returnedObjTypes;
      BoundStatement stmt = backend.buildStatement(SCAN_OBJS, config.repositoryId());
      rs = backend.execute(stmt).iterator();
    }

    @Override
    public void close() {}

    @Nullable
    @Override
    protected Obj computeNext() {
      while (true) {
        if (!rs.hasNext()) {
          return endOfData();
        }

        Row row = rs.next();
        ObjType type = ObjTypes.forName(requireNonNull(row.getString(1)));
        if (!returnedObjTypes.contains(type)) {
          continue;
        }

        ObjId id = deserializeObjId(row.getString(COL_OBJ_ID.name()));
        String versionToken = row.getString(COL_OBJ_VERS.name());
        return ObjSerializers.forType(type).deserialize(row, type, id, versionToken);
      }
    }
  }
}
