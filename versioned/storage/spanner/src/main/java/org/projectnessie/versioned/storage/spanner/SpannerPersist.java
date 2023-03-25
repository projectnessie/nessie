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
package org.projectnessie.versioned.storage.spanner;

import static com.google.cloud.spanner.ErrorCode.ALREADY_EXISTS;
import static com.google.cloud.spanner.Mutation.newInsertBuilder;
import static com.google.cloud.spanner.Mutation.newReplaceBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
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
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.common.util.Closing.closeMultiple;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.CHECK_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_CREATED;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_INCOMPLETE_INDEX;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_INCREMENTAL_INDEX;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_MESSAGE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_REFERENCE_INDEX;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_REFERENCE_INDEX_STRIPES;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_SECONDARY_PARENTS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_SEQ;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_TAIL;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_COMMIT_TYPE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_INDEX_INDEX;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REFS_DELETED;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REFS_NAME;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REFS_POINTER;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REF_CREATED_AT;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REF_INITIAL_POINTER;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REF_NAME;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_REPO_ID;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_SEGMENTS_STRIPES;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_STRING_COMPRESSION;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_STRING_CONTENT_TYPE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_STRING_FILENAME;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_STRING_PREDECESSORS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_STRING_TEXT;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_TAG_COMMIT_ID;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_TAG_HEADERS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_TAG_MESSAGE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_TAG_SIGNATURE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_VALUE_CONTENT_ID;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_VALUE_DATA;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.COL_VALUE_PAYLOAD;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.DELETE_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.FIND_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.FIND_OBJS_TYPED;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.FIND_REFERENCES;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.MARK_REFERENCE_AS_DELETED;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.PURGE_REFERENCE;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.SCAN_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.TABLE_REFS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.UPDATE_REFERENCE_POINTER;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.collections.ObjectHashSet;
import org.jetbrains.annotations.NotNull;
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

public class SpannerPersist implements Persist {

  private final SpannerBackend backend;
  private final StoreConfig config;

  SpannerPersist(SpannerBackend backend, StoreConfig config) {
    this.backend = backend;
    this.config = config;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return SpannerBackendFactory.NAME;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public StoreConfig config() {
    return config;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    try {
      backend
          .client()
          .write(
              singletonList(
                  newInsertBuilder(TABLE_REFS)
                      .set(COL_REPO_ID)
                      .to(config.repositoryId())
                      .set(COL_REFS_NAME)
                      .to(reference.name())
                      .set(COL_REFS_POINTER)
                      .to(reference.pointer().toString())
                      .set(COL_REFS_DELETED)
                      .to(reference.deleted())
                      .build()));
    } catch (SpannerException e) {
      if (requireNonNull(e.getErrorCode()) == ALREADY_EXISTS) {
        throw new RefAlreadyExistsException(fetchReference(reference.name()));
      }
      throw e;
    }

    return reference;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {

    conditionalReferenceUpdate(reference, MARK_REFERENCE_AS_DELETED);

    return reference(reference.name(), reference.pointer(), true);
  }

  @Override
  public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    conditionalReferenceUpdate(reference, PURGE_REFERENCE);
  }

  private void conditionalReferenceUpdate(
      @jakarta.annotation.Nonnull @Nonnull Reference reference, String markReferenceAsDeleted)
      throws RefNotFoundException, RefConditionFailedException {
    Long updated =
        backend
            .client()
            .readWriteTransaction()
            .run(
                tx ->
                    tx.executeUpdate(
                        Statement.newBuilder(markReferenceAsDeleted)
                            .bind("RepoID")
                            .to(config.repositoryId())
                            .bind("Name")
                            .to(reference.name())
                            .bind("Pointer")
                            .to(reference.pointer().toString())
                            .build()));
    if (updated != 1L) {
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

    Long updated =
        backend
            .client()
            .readWriteTransaction()
            .run(
                tx ->
                    tx.executeUpdate(
                        Statement.newBuilder(UPDATE_REFERENCE_POINTER)
                            .bind("NewPointer")
                            .to(newPointer.toString())
                            .bind("RepoID")
                            .to(config.repositoryId())
                            .bind("Name")
                            .to(reference.name())
                            .bind("Pointer")
                            .to(reference.pointer().toString())
                            .build()));
    if (updated != 1L) {
      Reference ref = fetchReference(reference.name());
      if (ref == null) {
        throw new RefNotFoundException(reference);
      }
      throw new RefConditionFailedException(ref);
    }

    return reference(reference.name(), newPointer, false);
  }

  @Override
  @Nullable
  @jakarta.annotation.Nullable
  public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
    return fetchReferences(new String[] {name})[0];
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
    try (ResultSet rs =
        backend
            .client()
            .singleUse() // Execute a single read or query against Cloud Spanner.
            .executeQuery(
                Statement.newBuilder(FIND_REFERENCES)
                    .bind("RepoID")
                    .to(config.repositoryId())
                    .bind("Names")
                    .toStringArray(asList(names))
                    .build())) {
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

      while (rs.next()) {
        Reference ref =
            reference(rs.getString(0), objIdFromString(rs.getString(1)), rs.getBoolean(2));
        int i = nameToIndex.getValue(ref.name());
        if (i != -1) {
          r[i] = ref;
        }
      }

      return r;
    }
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
  public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    return fetchObj(id).type();
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
  private Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids, ObjType type)
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

    Statement.Builder st =
        Statement.newBuilder(type == null ? FIND_OBJS : FIND_OBJS_TYPED)
            .bind("RepoID")
            .to(config.repositoryId())
            .bind("Ids")
            .toStringArray(stream(ids).map(ObjId::toString).collect(Collectors.toList()));
    if (type != null) {
      st = st.bind("Type").to(type.name());
    }
    try (ResultSet rs =
        backend
            .client()
            .singleUse() // Execute a single read or query against Cloud Spanner.
            .executeQuery(st.build())) {
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
  }

  private Obj deserializeObj(ResultSet rs) {
    ObjId id = deserializeObjId(rs, COL_OBJ_ID);
    String objType = rs.getString(COL_OBJ_TYPE);
    ObjType type = ObjType.valueOf(objType);

    @SuppressWarnings("rawtypes")
    StoreObjDesc objDesc = STORE_OBJ_TYPE.get(type);
    checkState(objDesc != null, "Cannot deserialize object type %s", objType);
    return objDesc.deserialize(rs, id);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public boolean[] storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    return upsertObjs(objs, false, true);
  }

  @Override
  public boolean storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    return upsertObjs(new Obj[] {obj}, ignoreSoftSizeRestrictions, true)[0];
  }

  @Override
  public void upsertObj(@Nonnull @jakarta.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    upsertObjs(new Obj[] {obj});
  }

  @Override
  public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    upsertObjs(objs, false, false);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private boolean[] upsertObjs(
      @Nonnull @jakarta.annotation.Nonnull Obj[] objs,
      boolean ignoreSoftSizeRestrictions,
      boolean insert)
      throws ObjTooLargeException {

    // Retry-loop when an ALREADY_EXISTS error occurs. Spanner does not have something like
    // INSERT ... IF NOT EXISTS or INSERT ... ON CONFLICT DO NOTHING.
    Set<ObjId> existing = emptySet();
    while (true) {
      boolean[] r = new boolean[objs.length];

      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < objs.length; i++) {
        Obj obj = objs[i];
        WriteBuilder mutation =
            insert ? newInsertBuilder(TABLE_OBJS) : newReplaceBuilder(TABLE_OBJS);
        if (obj == null) {
          continue;
        }

        ObjId id = obj.id();
        if (existing.contains(id)) {
          continue;
        }
        ObjType type = obj.type();

        int incrementalIndexSizeLimit =
            ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
        int indexSizeLimit =
            ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();

        checkArgument(id != null, "Obj to store must have a non-null ID");

        checkArgument(STORE_OBJ_TYPE.containsKey(type), "Cannot serialize object type %s ", type);

        mutation
            .set(COL_REPO_ID)
            .to(config.repositoryId())
            .set(COL_OBJ_ID)
            .to(id.toString())
            .set(COL_OBJ_TYPE)
            .to(type.name());

        @SuppressWarnings("rawtypes")
        StoreObjDesc storeType = STORE_OBJ_TYPE.get(type);
        //noinspection unchecked
        storeType.store(mutation, obj, incrementalIndexSizeLimit, indexSizeLimit);

        mutations.add(mutation.build());

        r[i] = true;
      }

      if (!mutations.isEmpty()) {
        try {
          backend.client().write(mutations);
        } catch (SpannerException e) {
          if (e.getErrorCode() == ALREADY_EXISTS) {
            Statement st =
                Statement.newBuilder(CHECK_OBJS)
                    .bind("RepoID")
                    .to(config.repositoryId())
                    .bind("Ids")
                    .toStringArray(
                        stream(objs)
                            .filter(Objects::nonNull)
                            .map(Obj::id)
                            .map(ObjId::toString)
                            .collect(Collectors.toList()))
                    .build();
            try (ResultSet rs = backend.client().singleUse().executeQuery(st)) {
              existing = new ObjectHashSet<>(objs.length * 2);
              while (rs.next()) {
                existing.add(deserializeObjId(rs, COL_OBJ_ID));
              }
            }
            continue;
          }
          throw e;
        }
      }
      return r;
    }
  }

  @Override
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    deleteObjs(new ObjId[] {id});
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    if (ids.length == 0) {
      return;
    }

    backend
        .client()
        .readWriteTransaction()
        .run(
            tx ->
                tx.executeUpdate(
                    Statement.newBuilder(DELETE_OBJS)
                        .bind("RepoID")
                        .to(config.repositoryId())
                        .bind("Ids")
                        .toStringArray(
                            Arrays.stream(ids).map(ObjId::toString).collect(Collectors.toList()))
                        .build()));
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config().repositoryId()));
  }

  @NotNull
  @Override
  public CloseableIterator<Obj> scanAllObjects(@NotNull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(backend.client(), returnedObjTypes);
  }

  private abstract static class StoreObjDesc<O extends Obj> {

    abstract O deserialize(ResultSet rs, ObjId id);

    abstract void store(
        WriteBuilder ps, O obj, int incrementalIndexLimit, int maxSerializedIndexSize)
        throws ObjTooLargeException;
  }

  private static final Map<ObjType, StoreObjDesc<?>> STORE_OBJ_TYPE = new EnumMap<>(ObjType.class);

  static {
    STORE_OBJ_TYPE.put(
        ObjType.COMMIT,
        new StoreObjDesc<CommitObj>() {

          @Override
          void store(
              WriteBuilder ps, CommitObj obj, int incrementalIndexLimit, int maxSerializedIndexSize)
              throws ObjTooLargeException {
            ps.set(COL_COMMIT_CREATED)
                .to(obj.created())
                .set(COL_COMMIT_SEQ)
                .to(obj.seq())
                .set(COL_COMMIT_MESSAGE)
                .to(obj.message());

            obj.headers();
            Headers.Builder hb = Headers.newBuilder();
            for (String h : obj.headers().keySet()) {
              hb.addHeaders(
                  HeaderEntry.newBuilder().setName(h).addAllValues(obj.headers().getAll(h)));
            }
            serializeHeaders(ps, COL_COMMIT_HEADERS, hb);

            serializeObjId(ps, COL_COMMIT_REFERENCE_INDEX, obj.referenceIndex());

            Stripes.Builder b = Stripes.newBuilder();
            obj.referenceIndexStripes().stream()
                .map(
                    s ->
                        Stripe.newBuilder()
                            .setFirstKey(s.firstKey().rawString())
                            .setLastKey(s.lastKey().rawString())
                            .setSegment(s.segment().asBytes()))
                .forEach(b::addStripes);
            ps.set(COL_COMMIT_REFERENCE_INDEX_STRIPES)
                .to(ByteArray.copyFrom(b.build().toByteArray()));

            serializeObjIds(ps, COL_COMMIT_TAIL, obj.tail());
            serializeObjIds(ps, COL_COMMIT_SECONDARY_PARENTS, obj.secondaryParents());

            ByteString index = obj.incrementalIndex();
            if (index.size() > incrementalIndexLimit) {
              throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
            }
            ps.set(COL_COMMIT_INCREMENTAL_INDEX).to(ByteArray.copyFrom(index.toByteArray()));

            ps.set(COL_COMMIT_INCOMPLETE_INDEX).to(obj.incompleteIndex());
            ps.set(COL_COMMIT_TYPE).to(obj.commitType().name());
          }

          @Override
          CommitObj deserialize(ResultSet rs, ObjId id) {
            CommitObj.Builder b =
                CommitObj.commitBuilder()
                    .id(id)
                    .created(rs.getLong(COL_COMMIT_CREATED))
                    .seq(rs.getLong(COL_COMMIT_SEQ))
                    .message(deserializeString(rs, COL_COMMIT_MESSAGE))
                    .referenceIndex(deserializeObjId(rs, COL_COMMIT_REFERENCE_INDEX))
                    .incrementalIndex(deserializeBytes(rs, COL_COMMIT_INCREMENTAL_INDEX))
                    .incompleteIndex(rs.getBoolean(COL_COMMIT_INCOMPLETE_INDEX))
                    .commitType(CommitType.valueOf(rs.getString(COL_COMMIT_TYPE)));
            deserializeObjIds(rs, COL_COMMIT_TAIL, b::addTail);
            deserializeObjIds(rs, COL_COMMIT_SECONDARY_PARENTS, b::addSecondaryParents);

            try {
              CommitHeaders.Builder h = CommitHeaders.newCommitHeaders();
              Headers headers =
                  Headers.parseFrom(rs.getBytes(COL_COMMIT_HEADERS).asReadOnlyByteBuffer());
              for (HeaderEntry e : headers.getHeadersList()) {
                for (String v : e.getValuesList()) {
                  h.add(e.getName(), v);
                }
              }
              b.headers(h.build());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            if (!rs.isNull(COL_COMMIT_REFERENCE_INDEX_STRIPES)) {
              try {
                Stripes stripes =
                    Stripes.parseFrom(
                        rs.getBytes(COL_COMMIT_REFERENCE_INDEX_STRIPES).asReadOnlyByteBuffer());
                stripes.getStripesList().stream()
                    .map(
                        s ->
                            indexStripe(
                                keyFromString(s.getFirstKey()),
                                keyFromString(s.getLastKey()),
                                ObjId.objIdFromByteArray(s.getSegment().toByteArray())))
                    .forEach(b::addReferenceIndexStripes);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            return b.build();
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.REF,
        new StoreObjDesc<RefObj>() {

          @Override
          void store(
              WriteBuilder ps, RefObj obj, int incrementalIndexLimit, int maxSerializedIndexSize) {
            ps.set(COL_REF_NAME)
                .to(obj.name())
                .set(COL_REF_INITIAL_POINTER)
                .to(obj.initialPointer().toString())
                .set(COL_REF_CREATED_AT)
                .to(obj.createdAtMicros());
          }

          @Override
          RefObj deserialize(ResultSet rs, ObjId id) {
            return ref(
                id,
                rs.getString(COL_REF_NAME),
                deserializeObjId(rs, COL_REF_INITIAL_POINTER),
                rs.getLong(COL_REF_CREATED_AT));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.VALUE,
        new StoreObjDesc<ContentValueObj>() {

          @Override
          void store(
              WriteBuilder ps,
              ContentValueObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            if (obj.contentId() != null) {
              ps.set(COL_VALUE_CONTENT_ID).to(obj.contentId());
            }
            ps.set(COL_VALUE_PAYLOAD).to(obj.payload());
            serializeBytes(ps, COL_VALUE_DATA, obj.data());
          }

          @Override
          ContentValueObj deserialize(ResultSet rs, ObjId id) {
            return contentValue(
                id,
                deserializeString(rs, COL_VALUE_CONTENT_ID),
                (int) rs.getLong(COL_VALUE_PAYLOAD),
                requireNonNull(deserializeBytes(rs, COL_VALUE_DATA)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.INDEX_SEGMENTS,
        new StoreObjDesc<IndexSegmentsObj>() {

          @Override
          void store(
              WriteBuilder ps,
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
            serializeBytes(ps, COL_SEGMENTS_STRIPES, b.build().toByteString());
          }

          @Override
          IndexSegmentsObj deserialize(ResultSet rs, ObjId id) {
            try {
              Stripes stripes =
                  Stripes.parseFrom(rs.getBytes(COL_SEGMENTS_STRIPES).asReadOnlyByteBuffer());
              List<IndexStripe> stripeList =
                  stripes.getStripesList().stream()
                      .map(
                          s ->
                              indexStripe(
                                  keyFromString(s.getFirstKey()),
                                  keyFromString(s.getLastKey()),
                                  ObjId.objIdFromByteArray(s.getSegment().toByteArray())))
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
          void store(
              WriteBuilder ps, IndexObj obj, int incrementalIndexLimit, int maxSerializedIndexSize)
              throws ObjTooLargeException {
            ByteString index = obj.index();
            if (index.size() > maxSerializedIndexSize) {
              throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
            }
            serializeBytes(ps, COL_INDEX_INDEX, index);
          }

          @Override
          IndexObj deserialize(ResultSet rs, ObjId id) {
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
          void store(
              WriteBuilder ps, TagObj obj, int incrementalIndexLimit, int maxSerializedIndexSize) {
            serializeObjId(ps, COL_TAG_COMMIT_ID, obj.commitId());
            ps.set(COL_TAG_MESSAGE).to(obj.message());
            Headers.Builder hb = Headers.newBuilder();
            CommitHeaders headers = obj.headers();
            if (headers != null) {
              for (String h : headers.keySet()) {
                hb.addHeaders(HeaderEntry.newBuilder().setName(h).addAllValues(headers.getAll(h)));
              }
            }
            serializeHeaders(ps, COL_TAG_HEADERS, hb);
            serializeBytes(ps, COL_TAG_SIGNATURE, obj.signature());
          }

          @Override
          TagObj deserialize(ResultSet rs, ObjId id) {
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
                deserializeObjId(rs, COL_TAG_COMMIT_ID),
                deserializeString(rs, COL_TAG_MESSAGE),
                tagHeaders,
                deserializeBytes(rs, COL_TAG_SIGNATURE));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.STRING,
        new StoreObjDesc<StringObj>() {

          @Override
          void store(
              WriteBuilder ps,
              StringObj obj,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            ps.set(COL_STRING_CONTENT_TYPE)
                .to(obj.contentType())
                .set(COL_STRING_COMPRESSION)
                .to(obj.compression().name())
                .set(COL_STRING_FILENAME)
                .to(obj.filename());
            serializeObjIds(ps, COL_STRING_PREDECESSORS, obj.predecessors());
            serializeBytes(ps, COL_STRING_TEXT, obj.text());
          }

          @Override
          StringObj deserialize(ResultSet rs, ObjId id) {
            return stringData(
                id,
                rs.getString(COL_STRING_CONTENT_TYPE),
                Compression.valueOf(rs.getString(COL_STRING_COMPRESSION)),
                deserializeString(rs, COL_STRING_FILENAME),
                deserializeObjIds(rs, COL_STRING_PREDECESSORS),
                deserializeBytes(rs, COL_STRING_TEXT));
          }
        });
  }

  private static String deserializeString(ResultSet rs, String col) {
    if (rs.isNull(col)) {
      return null;
    }
    return rs.getString(col);
  }

  private static void serializeHeaders(WriteBuilder ps, String name, Headers.Builder hb) {
    ps.set(name).to(ByteArray.copyFrom(hb.build().toByteArray()));
  }

  private static void serializeBytes(WriteBuilder ps, String name, ByteString binary) {
    if (binary != null) {
      ps.set(name).to(ByteArray.copyFrom(binary.toByteArray()));
    }
  }

  private static ByteString deserializeBytes(ResultSet rs, String col) {
    if (rs.isNull(col)) {
      return null;
    }
    return unsafeWrap(rs.getBytes(col).asReadOnlyByteBuffer());
  }

  private static void serializeObjId(WriteBuilder ps, String name, ObjId id) {
    if (id != null) {
      ps.set(name).to(id.toString());
    }
  }

  private static ObjId deserializeObjId(ResultSet rs, String col) {
    if (rs.isNull(col)) {
      return null;
    }
    return objIdFromString(rs.getString(col));
  }

  private static void serializeObjIds(WriteBuilder ps, String name, List<ObjId> ids) {
    if (ids != null) {
      ps.set(name).toStringArray(ids.stream().map(ObjId::toString).collect(Collectors.toList()));
    }
  }

  private static List<ObjId> deserializeObjIds(ResultSet rs, String col) {
    if (rs.isNull(col)) {
      return null;
    }
    return rs.getStringList(col).stream().map(ObjId::objIdFromString).collect(Collectors.toList());
  }

  private static void deserializeObjIds(ResultSet rs, String col, Consumer<ObjId> idConsumer) {
    if (rs.isNull(col)) {
      return;
    }
    for (String id : rs.getStringList(col)) {
      idConsumer.accept(objIdFromString(id));
    }
  }

  private class ScanAllObjectsIterator extends ResultSetIterator<Obj> {
    ScanAllObjectsIterator(DatabaseClient client, Set<ObjType> returnedObjTypes) {
      super(
          client,
          SCAN_OBJS,
          ps ->
              ps.bind("RepoID")
                  .to(config.repositoryId())
                  .bind("Types")
                  .toStringArray(
                      returnedObjTypes.stream().map(ObjType::name).collect(Collectors.toList())));
    }

    @Override
    protected Obj mapToObj(ResultSet rs) {
      return deserializeObj(rs);
    }
  }

  private abstract static class ResultSetIterator<R> extends AbstractIterator<R>
      implements CloseableIterator<R> {

    private final ResultSet rs;
    private final ReadContext readContext;

    ResultSetIterator(
        DatabaseClient client,
        String sql,
        Function<Statement.Builder, Statement.Builder> preparer) {
      readContext = client.singleUse();
      rs = readContext.executeQuery(preparer.apply(Statement.newBuilder(sql)).build());
    }

    @Override
    public void close() {
      List<AutoCloseable> c = new ArrayList<>();
      c.add(rs);
      c.add(readContext);
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
      if (!rs.next()) {
        return endOfData();
      }

      return mapToObj(rs);
    }

    protected abstract R mapToObj(ResultSet rs);
  }
}
