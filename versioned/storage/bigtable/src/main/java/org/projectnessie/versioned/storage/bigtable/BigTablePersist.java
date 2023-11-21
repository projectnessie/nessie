/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.CELL_TIMESTAMP;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.FAMILY_REFS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.MAX_PARALLEL_READS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_OBJS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_OBJ_TYPE;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.QUALIFIER_REFS;
import static org.projectnessie.versioned.storage.bigtable.BigTableConstants.READ_TIMEOUT_MILLIS;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.common.collect.AbstractIterator;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
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
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

public class BigTablePersist implements Persist {

  private final BigTableBackend backend;
  private final StoreConfig config;
  private final ByteString keyPrefix;

  BigTablePersist(BigTableBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
    this.keyPrefix = copyFromUtf8(config.repositoryId() + ':');
  }

  static RuntimeException apiException(ApiException e) {
    throw new RuntimeException("Unhandled BigTable exception", e);
  }

  private ByteString dbKey(ByteString key) {
    return keyPrefix.concat(key);
  }

  private ByteString dbKey(String key) {
    return dbKey(copyFromUtf8(key));
  }

  private ByteString dbKey(ObjId id) {
    return dbKey(unsafeWrap(id.asByteArray()));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return BigTableBackendFactory.NAME;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public StoreConfig config() {
    return config;
  }

  @Override
  public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
    try {
      ByteString key = dbKey(name);
      Row row = backend.client().readRow(backend.tableRefs, key);
      return row != null ? referenceFromRow(row) : null;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
    try {
      Reference[] r = new Reference[names.length];
      bulkFetch(
          backend.tableRefs, names, r, this::dbKey, BigTablePersist::referenceFromRow, name -> {});
      return r;
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    try {
      ByteString key = dbKey(reference.name());

      Mutation mutation = refsMutation(reference);
      Filter condition =
          FILTERS
              .chain()
              .filter(FILTERS.family().exactMatch(FAMILY_REFS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_REFS));

      boolean success =
          backend
              .client()
              .checkAndMutateRow(
                  ConditionalRowMutation.create(backend.tableRefs, key)
                      .condition(condition)
                      .otherwise(mutation));

      if (success) {
        throw new RefAlreadyExistsException(fetchReference(reference.name()));
      }

      return reference;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      ByteString key = dbKey(reference.name());

      Reference expected = reference.withDeleted(false);
      Reference deleted = reference.withDeleted(true);

      casReferenceAndThrow(reference, key, expected, refsMutation(deleted));
      return deleted;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Reference updateReferencePointer(
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      ByteString key = dbKey(reference.name());

      Reference expected = reference.withDeleted(false);
      Reference updated = reference.forNewPointer(newPointer, config);

      casReferenceAndThrow(reference, key, expected, refsMutation(updated));
      return updated;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    try {
      ByteString key = dbKey(reference.name());

      Reference expected = reference.withDeleted(true);

      casReferenceAndThrow(reference, key, expected, Mutation.create().deleteRow());
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @NotNull
  private static Mutation refsMutation(@NotNull Reference reference) {
    return Mutation.create()
        .setCell(
            FAMILY_REFS,
            QUALIFIER_REFS,
            // Note: must use a constant timestamp, otherwise BigTable will pile up historic values,
            // which would also break our CAS conditions, because historic values could match.
            CELL_TIMESTAMP,
            unsafeWrap(serializeReference(reference)));
  }

  @NotNull
  private static Filters.ChainFilter refsValueFilter(Reference expected) {
    return FILTERS
        .chain()
        .filter(FILTERS.family().exactMatch(FAMILY_REFS))
        .filter(FILTERS.qualifier().exactMatch(QUALIFIER_REFS))
        .filter(FILTERS.value().exactMatch(unsafeWrap(serializeReference(expected))));
  }

  private void casReferenceAndThrow(
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      ByteString key,
      Reference expected,
      Mutation mutation)
      throws RefConditionFailedException, RefNotFoundException {
    if (!casReference(key, refsValueFilter(expected), mutation)) {
      Reference r = fetchReference(reference.name());
      if (r != null) {
        throw new RefConditionFailedException(r);
      } else {
        throw new RefNotFoundException(reference);
      }
    }
  }

  private Boolean casReference(ByteString key, Filter condition, Mutation mutation) {
    return backend
        .client()
        .checkAndMutateRow(
            ConditionalRowMutation.create(backend.tableRefs, key)
                .condition(condition)
                .then(mutation));
  }

  private static Reference referenceFromRow(Row row) {
    List<RowCell> cells = row.getCells(FAMILY_REFS);
    for (RowCell cell : cells) {
      if (cell.getQualifier().equals(QUALIFIER_REFS)) {
        return deserializeReference(cell.getValue().toByteArray());
      }
    }
    throw new IllegalStateException("Row has no CF " + FAMILY_REFS);
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Obj fetchObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) throws ObjNotFoundException {
    try {
      ByteString key = dbKey(id);

      Row row = backend.client().readRow(backend.tableObjs, key);
      if (row != null) {
        ByteBuffer obj =
            row.getCells(FAMILY_OBJS, QUALIFIER_OBJS).get(0).getValue().asReadOnlyByteBuffer();
        return deserializeObj(id, obj);
      }
      throw new ObjNotFoundException(id);
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull @jakarta.annotation.Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    Obj obj = fetchObj(id);
    if (obj.type() != type) {
      throw new ObjNotFoundException(id);
    }
    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    return fetchObj(id).type();
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
      throws ObjNotFoundException {
    try {
      Obj[] r = new Obj[ids.length];
      List<ObjId> notFound = new ArrayList<>();
      bulkFetch(
          backend.tableObjs,
          ids,
          r,
          this::dbKey,
          row -> {
            ByteString key = row.getKey().substring(keyPrefix.size());
            ObjId id = deserializeObjId(key);
            ByteBuffer data =
                row.getCells(FAMILY_OBJS, QUALIFIER_OBJS).get(0).getValue().asReadOnlyByteBuffer();
            return deserializeObj(id, data);
          },
          notFound::add);

      if (!notFound.isEmpty()) {
        throw new ObjNotFoundException(notFound);
      }

      return r;
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {

    try {
      ConditionalRowMutation conditionalRowMutation =
          mutationForStoreObj(obj, ignoreSoftSizeRestrictions);

      boolean success = backend.client().checkAndMutateRow(conditionalRowMutation);
      return !success;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  private static final ByteString[] OBJ_TYPE_VALUES =
      Arrays.stream(ObjType.values())
          .map(Enum::name)
          .map(ByteString::copyFromUtf8)
          .toArray(ByteString[]::new);

  @NotNull
  private ConditionalRowMutation mutationForStoreObj(
      @NotNull Obj obj, boolean ignoreSoftSizeRestrictions) throws ObjTooLargeException {
    checkArgument(obj.id() != null, "Obj to store must have a non-null ID");
    ByteString key = dbKey(obj.id());

    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    byte[] serialized = serializeObj(obj, incrementalIndexSizeLimit, indexSizeLimit);

    ByteString ref = unsafeWrap(serialized);

    Mutation mutation =
        Mutation.create()
            .setCell(FAMILY_OBJS, QUALIFIER_OBJS, CELL_TIMESTAMP, ref)
            .setCell(
                FAMILY_OBJS,
                QUALIFIER_OBJ_TYPE,
                CELL_TIMESTAMP,
                OBJ_TYPE_VALUES[obj.type().ordinal()]);
    Filter condition =
        FILTERS
            .chain()
            .filter(FILTERS.key().exactMatch(key))
            .filter(FILTERS.family().exactMatch(FAMILY_OBJS))
            .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJS));
    return ConditionalRowMutation.create(backend.tableObjs, key)
        .condition(condition)
        .otherwise(mutation);
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public boolean[] storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    if (objs.length == 0) {
      return new boolean[0];
    }

    if (objs.length == 1) {
      Obj obj = objs[0];
      boolean r = obj != null && storeObj(obj);
      return new boolean[] {r};
    }

    @SuppressWarnings("unchecked")
    ApiFuture<Boolean>[] futures = new ApiFuture[objs.length];
    int idx = 0;
    for (int i = 0; i < objs.length; i++) {
      Obj obj = objs[i];
      if (obj != null) {
        ConditionalRowMutation conditionalRowMutation = mutationForStoreObj(obj, false);
        futures[idx] = backend.client().checkAndMutateRowAsync(conditionalRowMutation);
      }
      idx++;
    }

    boolean[] r = new boolean[objs.length];
    for (int i = 0; i < objs.length; i++) {
      Obj o = objs[i];
      if (o != null) {
        try {
          r[i] = !futures[i].get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return r;
  }

  @Override
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    try {
      ByteString key = dbKey(id);
      backend
          .client()
          .mutateRow(RowMutation.create(backend.tableObjs, key, Mutation.create().deleteRow()));
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    if (ids.length == 0) {
      return;
    }
    try (Batcher<RowMutationEntry, Void> batcher =
        backend.client().newBulkMutationBatcher(backend.tableObjs)) {
      for (ObjId id : ids) {
        if (id != null) {
          ByteString key = dbKey(id);
          batcher.add(RowMutationEntry.create(key).deleteRow());
        }
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void upsertObj(@Nonnull @jakarta.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    try {
      ByteString key = dbKey(id);

      ByteString serialized =
          unsafeWrap(
              serializeObj(
                  obj, effectiveIncrementalIndexSizeLimit(), effectiveIndexSegmentSizeLimit()));

      backend
          .client()
          .mutateRow(
              RowMutation.create(backend.tableObjs, key)
                  .setCell(FAMILY_OBJS, QUALIFIER_OBJS, CELL_TIMESTAMP, serialized)
                  .setCell(
                      FAMILY_OBJS,
                      QUALIFIER_OBJ_TYPE,
                      CELL_TIMESTAMP,
                      OBJ_TYPE_VALUES[obj.type().ordinal()]));
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    if (objs.length == 0) {
      return;
    }

    try (Batcher<RowMutationEntry, Void> batcher =
        backend.client().newBulkMutationBatcher(backend.tableObjs)) {
      for (Obj obj : objs) {
        if (obj == null) {
          continue;
        }
        ObjId id = obj.id();
        checkArgument(id != null, "Obj to store must have a non-null ID");

        ByteString key = dbKey(id);

        ByteString serialized =
            unsafeWrap(
                serializeObj(
                    obj, effectiveIncrementalIndexSizeLimit(), effectiveIndexSegmentSizeLimit()));

        batcher.add(
            RowMutationEntry.create(key)
                .setCell(FAMILY_OBJS, QUALIFIER_OBJS, CELL_TIMESTAMP, serialized)
                .setCell(
                    FAMILY_OBJS,
                    QUALIFIER_OBJ_TYPE,
                    CELL_TIMESTAMP,
                    OBJ_TYPE_VALUES[obj.type().ordinal()]));
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(
      @Nonnull @jakarta.annotation.Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes::contains);
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final Query.QueryPaginator paginator;
    private Iterator<Row> iter;

    private ByteString lastKey;

    ScanAllObjectsIterator(Predicate<ObjType> filter) {

      Query q = Query.create(backend.tableObjs).prefix(keyPrefix);

      Filters.ChainFilter filterChain =
          FILTERS.chain().filter(FILTERS.family().exactMatch(FAMILY_OBJS));

      Filters.InterleaveFilter typeFilter = null;
      boolean all = true;
      for (ObjType type : ObjType.values()) {
        boolean match = filter.test(type);
        if (match) {
          if (typeFilter == null) {
            typeFilter = FILTERS.interleave();
          }
          typeFilter.filter(
              FILTERS
                  .chain()
                  .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_TYPE))
                  .filter(FILTERS.value().exactMatch(OBJ_TYPE_VALUES[type.ordinal()])));
        } else {
          all = false;
        }
      }
      if (typeFilter == null) {
        throw new IllegalArgumentException("No object types matched the provided predicate");
      }
      if (!all) {
        // Condition filters are generally not recommended because they are slower, but
        // scanAllObjects is not meant to be particularly efficient. The fact that we are also
        // limiting the query to a row prefix should alleviate the performance impact.
        filterChain.filter(
            FILTERS.condition(typeFilter).then(FILTERS.pass()).otherwise(FILTERS.block()));
      }

      filterChain.filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJS));

      this.paginator = q.filter(filterChain).createPaginator(100);
      this.iter = backend.client().readRows(paginator.getNextQuery()).iterator();
    }

    @Override
    public void close() {}

    @Override
    protected Obj computeNext() {
      while (true) {
        if (!iter.hasNext()) {
          if (lastKey == null) {
            return endOfData();
          }
          paginator.advance(lastKey);
          iter = backend.client().readRows(paginator.getNextQuery()).iterator();
          lastKey = null;
          continue;
        }

        Row row = iter.next();

        ByteString key = row.getKey();
        lastKey = key;

        ObjId id = deserializeObjId(key.substring(keyPrefix.size()));

        List<RowCell> objCells = row.getCells(FAMILY_OBJS, QUALIFIER_OBJS);
        ByteBuffer obj = objCells.get(0).getValue().asReadOnlyByteBuffer();
        return deserializeObj(id, obj);
      }
    }
  }

  public static ObjId deserializeObjId(ByteString bytes) {
    if (bytes == null) {
      return null;
    }
    return objIdFromByteBuffer(bytes.asReadOnlyByteBuffer());
  }

  private <ID, R> void bulkFetch(
      String tableId,
      ID[] ids,
      R[] r,
      Function<ID, ByteString> keyGen,
      Function<Row, R> resultGen,
      Consumer<ID> notFound)
      throws InterruptedException, ExecutionException, TimeoutException {
    int num = ids.length;
    if (num == 0) {
      return;
    }

    ApiFuture<Row>[] handles;
    if (num <= MAX_PARALLEL_READS) {
      handles = doBulkFetch(ids, keyGen, key -> backend.client().readRowAsync(tableId, key));
    } else {
      try (Batcher<ByteString, Row> batcher = backend.client().newBulkReadRowsBatcher(tableId)) {
        handles = doBulkFetch(ids, keyGen, batcher::add);
      }
    }

    for (int idx = 0; idx < num; idx++) {
      ApiFuture<Row> handle = handles[idx];
      if (handle != null) {
        Row row = handle.get(READ_TIMEOUT_MILLIS, MILLISECONDS);
        if (row != null) {
          r[idx] = resultGen.apply(row);
        } else {
          notFound.accept(ids[idx]);
        }
      }
    }
  }

  private <ID> ApiFuture<Row>[] doBulkFetch(
      ID[] ids, Function<ID, ByteString> keyGen, Function<ByteString, ApiFuture<Row>> handleGen) {
    int num = ids.length;
    @SuppressWarnings("unchecked")
    ApiFuture<Row>[] handles = new ApiFuture[num];
    for (int idx = 0; idx < num; idx++) {
      ID id = ids[idx];
      if (id != null) {
        ByteString key = keyGen.apply(id);
        handles[idx] = handleGen.apply(key);
      }
    }
    return handles;
  }
}
