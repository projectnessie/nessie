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
package org.projectnessie.versioned.storage.mongodb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.mongodb.ErrorCategory.DUPLICATE_KEY;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Updates.set;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.projectnessie.versioned.storage.common.persist.ObjTypes.objTypeByName;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_REFERENCED;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_VERS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_CREATED_AT;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_DELETED;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_NAME;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_POINTER;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_PREVIOUS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REPO;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.ID_PROPERTY_NAME;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.ID_REPO_PATH;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToObjId;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.objIdToBinary;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;

import com.google.common.collect.AbstractIterator;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.UnknownOperationResultException;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.mongodb.serializers.ObjSerializer;
import org.projectnessie.versioned.storage.mongodb.serializers.ObjSerializers;

public class MongoDBPersist implements Persist {

  private final StoreConfig config;
  private final MongoDBBackend backend;

  MongoDBPersist(MongoDBBackend backend, StoreConfig config) {
    this.config = config;
    this.backend = backend;
  }

  @Nonnull
  @Override
  public String name() {
    return MongoDBBackendFactory.NAME;
  }

  @Nonnull
  @Override
  public StoreConfig config() {
    return config;
  }

  private Document idRefDoc(Reference reference) {
    return idRefDoc(reference.name());
  }

  private Document idRefDoc(String name) {
    Document idDoc = new Document();
    idDoc.put(COL_REPO, config.repositoryId());
    idDoc.put(COL_REFERENCES_NAME, name);
    return idDoc;
  }

  private Document idObjDoc(ObjId id) {
    Document idDoc = new Document();
    idDoc.put(COL_REPO, config.repositoryId());
    idDoc.put(COL_OBJ_ID, objIdToBinary(id));
    return idDoc;
  }

  @Nonnull
  @Override
  public Reference addReference(@Nonnull Reference reference) throws RefAlreadyExistsException {
    checkArgument(!reference.deleted(), "Deleted references must not be added");

    Document doc = new Document();
    doc.put(ID_PROPERTY_NAME, idRefDoc(reference));
    doc.put(COL_REFERENCES_POINTER, objIdToBinary(reference.pointer()));
    doc.put(COL_REFERENCES_DELETED, reference.deleted());
    long createdAtMicros = reference.createdAtMicros();
    if (createdAtMicros != 0L) {
      doc.put(COL_REFERENCES_CREATED_AT, createdAtMicros);
    }
    ObjId extendedInfoObj = reference.extendedInfoObj();
    if (extendedInfoObj != null) {
      doc.put(COL_REFERENCES_EXTENDED_INFO, objIdToBinary(extendedInfoObj));
    }
    byte[] previous = serializePreviousPointers(reference.previousPointers());
    if (previous != null) {
      doc.put(COL_REFERENCES_PREVIOUS, new Binary(previous));
    }
    try {
      backend.refs().insertOne(doc);
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == DUPLICATE_KEY) {
        throw new RefAlreadyExistsException(fetchReference(reference.name()));
      }
      throw unhandledException(e);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    return reference;
  }

  @Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(false);

    UpdateResult result;
    try {
      result =
          backend
              .refs()
              .updateOne(referenceCondition(reference), set(COL_REFERENCES_DELETED, true));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    if (result.getModifiedCount() != 1) {
      Reference ex = fetchReference(reference.name());
      if (ex == null) {
        throw new RefNotFoundException(reference.name());
      }
      throw new RefConditionFailedException(ex);
    }

    return reference.withDeleted(true);
  }

  private Bson referenceCondition(Reference reference) {
    List<Bson> filters = new ArrayList<>(5);
    filters.add(eq(ID_PROPERTY_NAME, idRefDoc(reference)));
    filters.add(eq(COL_REFERENCES_POINTER, objIdToBinary(reference.pointer())));
    filters.add(eq(COL_REFERENCES_DELETED, reference.deleted()));
    long createdAt = reference.createdAtMicros();
    if (createdAt != 0L) {
      filters.add(eq(COL_REFERENCES_CREATED_AT, createdAt));
    } else {
      filters.add(not(exists(COL_REFERENCES_CREATED_AT)));
    }
    ObjId extendedInfoObj = reference.extendedInfoObj();
    if (extendedInfoObj != null) {
      filters.add(eq(COL_REFERENCES_EXTENDED_INFO, objIdToBinary(extendedInfoObj)));
    } else {
      filters.add(not(exists(COL_REFERENCES_EXTENDED_INFO)));
    }

    return and(filters);
  }

  @Nonnull
  @Override
  public Reference updateReferencePointer(@Nonnull Reference reference, @Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(false);

    Reference updated = reference.forNewPointer(newPointer, config);
    List<Bson> updates = new ArrayList<>();
    updates.add(set(COL_REFERENCES_POINTER, objIdToBinary(newPointer)));
    byte[] previous = serializePreviousPointers(updated.previousPointers());
    if (previous != null) {
      updates.add(set(COL_REFERENCES_PREVIOUS, new Binary(previous)));
    }

    UpdateResult result;
    try {
      result = backend.refs().updateOne(referenceCondition(reference), updates);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    if (result.getModifiedCount() != 1) {
      if (result.getMatchedCount() == 1) {
        // not updated
        return updated;
      }

      Reference ex = fetchReference(reference.name());
      if (ex == null) {
        throw new RefNotFoundException(reference.name());
      }
      throw new RefConditionFailedException(ex);
    }

    return updated;
  }

  @Override
  public void purgeReference(@Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(true);

    DeleteResult result;
    try {
      result = backend.refs().deleteOne(referenceCondition(reference));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    if (result.getDeletedCount() != 1) {
      Reference ex = fetchReference(reference.name());
      if (ex == null) {
        throw new RefNotFoundException(reference.name());
      }
      throw new RefConditionFailedException(ex);
    }
  }

  @Override
  public Reference fetchReference(@Nonnull String name) {
    FindIterable<Document> result;
    try {
      result = backend.refs().find(eq(ID_PROPERTY_NAME, idRefDoc(name)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    Document doc = result.first();
    if (doc == null) {
      return null;
    }

    Binary prev = doc.get(COL_REFERENCES_PREVIOUS, Binary.class);
    List<Reference.PreviousPointer> previous =
        prev != null ? deserializePreviousPointers(prev.getData()) : emptyList();
    return reference(
        name,
        binaryToObjId(doc.get(COL_REFERENCES_POINTER, Binary.class)),
        doc.getBoolean(COL_REFERENCES_DELETED),
        refCreatedAt(doc),
        binaryToObjId(doc.get(COL_REFERENCES_EXTENDED_INFO, Binary.class)),
        previous);
  }

  private static Long refCreatedAt(Document doc) {
    return doc.containsKey(COL_REFERENCES_CREATED_AT) ? doc.getLong(COL_REFERENCES_CREATED_AT) : 0L;
  }

  @Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull String[] names) {
    List<Document> nameIdDocs =
        Arrays.stream(names).filter(Objects::nonNull).map(this::idRefDoc).collect(toList());
    FindIterable<Document> result;
    try {
      result = backend.refs().find(in(ID_PROPERTY_NAME, nameIdDocs));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    Reference[] r = new Reference[names.length];

    for (Document doc : result) {
      String name = doc.get(ID_PROPERTY_NAME, Document.class).getString(COL_REFERENCES_NAME);
      Binary prev = doc.get(COL_REFERENCES_PREVIOUS, Binary.class);
      List<Reference.PreviousPointer> previous =
          prev != null ? deserializePreviousPointers(prev.getData()) : emptyList();
      Reference reference =
          reference(
              name,
              binaryToObjId(doc.get(COL_REFERENCES_POINTER, Binary.class)),
              doc.getBoolean(COL_REFERENCES_DELETED),
              refCreatedAt(doc),
              binaryToObjId(doc.get(COL_REFERENCES_EXTENDED_INFO, Binary.class)),
              previous);
      for (int i = 0; i < names.length; i++) {
        if (name.equals(names[i])) {
          r[i] = reference;
        }
      }
    }

    return r;
  }

  @Override
  @Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass) throws ObjNotFoundException {
    FindIterable<Document> result;
    try {
      result =
          type != null
              ? backend
                  .objs()
                  .find(and(eq(ID_PROPERTY_NAME, idObjDoc(id)), eq(COL_OBJ_TYPE, type.shortName())))
              : backend.objs().find(eq(ID_PROPERTY_NAME, idObjDoc(id)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    Document doc = result.first();

    T obj = docToObj(id, doc, type, typeClass);
    if (obj == null) {
      throw new ObjNotFoundException(id);
    }
    return obj;
  }

  @Override
  @Nonnull
  public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
    FindIterable<Document> result;
    try {
      result = backend.objs().find(eq(ID_PROPERTY_NAME, idObjDoc(id)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    Document doc = result.first();
    if (doc == null) {
      throw new ObjNotFoundException(id);
    }

    return objTypeByName(doc.getString(COL_OBJ_TYPE));
  }

  @Override
  public <T extends Obj> T[] fetchTypedObjsIfExist(
      @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
    List<Document> list = new ArrayList<>(ids.length);
    Object2IntHashMap<ObjId> idToIndex =
        new Object2IntHashMap<>(ids.length * 2, Hashing.DEFAULT_LOAD_FACTOR, -1);
    @SuppressWarnings("unchecked")
    T[] r = (T[]) Array.newInstance(typeClass, ids.length);
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null) {
        list.add(idObjDoc(id));
        idToIndex.put(id, i);
      }
    }

    if (!list.isEmpty()) {
      fetchObjsPage(r, list, idToIndex, type, typeClass);
    }

    return r;
  }

  private <T extends Obj> void fetchObjsPage(
      Obj[] r,
      List<Document> list,
      Object2IntHashMap<ObjId> idToIndex,
      ObjType type,
      Class<T> typeClass) {
    FindIterable<Document> result;
    try {
      result = backend.objs().find(in(ID_PROPERTY_NAME, list));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    for (Document doc : result) {
      T obj = docToObj(doc, type, typeClass);
      if (obj != null) {
        int idx = idToIndex.getValue(obj.id());
        if (idx != -1) {
          r[idx] = obj;
        }
      }
    }
  }

  @Override
  public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    long referenced = config.currentTimeMicros();
    Document doc = objToDoc(obj, referenced, ignoreSoftSizeRestrictions);
    try {
      backend.objs().insertOne(doc);
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == DUPLICATE_KEY) {
        backend
            .objs()
            .updateOne(
                eq(ID_PROPERTY_NAME, idObjDoc(obj.id())),
                Updates.set(COL_OBJ_REFERENCED, referenced));
        return false;
      }
      throw handleMongoWriteException(e);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }

    return true;
  }

  @Nonnull
  @Override
  public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    boolean[] r = new boolean[objs.length];

    storeObjsWrite(objs, r);

    List<ObjId> updateReferenced = new ArrayList<>();
    for (int i = 0; i < r.length; i++) {
      if (!r[i]) {
        Obj obj = objs[i];
        if (obj != null) {
          updateReferenced.add(obj.id());
        }
      }
    }

    if (!updateReferenced.isEmpty()) {
      storeObjsUpdateReferenced(objs, updateReferenced);
    }

    return r;
  }

  private void storeObjsUpdateReferenced(Obj[] objs, List<ObjId> updateReferenced) {
    long referenced = config.currentTimeMicros();
    List<UpdateOneModel<Document>> docs =
        updateReferenced.stream()
            .map(
                id ->
                    new UpdateOneModel<Document>(
                        eq(ID_PROPERTY_NAME, idObjDoc(id)),
                        Updates.set(COL_OBJ_REFERENCED, referenced)))
            .collect(toList());
    List<WriteModel<Document>> updates = new ArrayList<>(docs);
    while (!updates.isEmpty()) {
      try {
        backend.objs().bulkWrite(updates);
        break;
      } catch (MongoBulkWriteException e) {
        // Handle "insert of already existing objects".
        //
        // MongoDB returns a BulkWriteResult of what _would_ have succeeded. Use that information
        // to retry the bulk write to make progress.
        List<BulkWriteError> errs = e.getWriteErrors();
        for (BulkWriteError err : errs) {
          throw handleMongoWriteError(e, err);
        }
        BulkWriteResult res = e.getWriteResult();
        updates.clear();
        res.getUpserts().stream()
            .map(MongoDBPersist::objIdFromBulkWriteUpsert)
            .mapToInt(id -> objIdIndex(objs, id))
            .mapToObj(docs::get)
            .forEach(updates::add);
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
    }
  }

  private void storeObjsWrite(Obj[] objs, boolean[] r) throws ObjTooLargeException {
    List<WriteModel<Document>> docs = new ArrayList<>(objs.length);
    long referenced = config.currentTimeMicros();
    for (Obj obj : objs) {
      if (obj != null) {
        docs.add(new InsertOneModel<>(objToDoc(obj, referenced, false)));
      }
    }
    List<WriteModel<Document>> inserts = new ArrayList<>(docs);
    while (!inserts.isEmpty()) {
      try {
        BulkWriteResult res = backend.objs().bulkWrite(inserts);
        for (BulkWriteInsert insert : res.getInserts()) {
          ObjId id = objIdFromBulkWriteInsert(insert);
          r[objIdIndex(objs, id)] = true;
        }
        break;
      } catch (MongoBulkWriteException e) {
        // Handle "insert of already existing objects".
        //
        // MongoDB returns a BulkWriteResult of what _would_ have succeeded. Use that information
        // to retry the bulk write to make progress.
        List<BulkWriteError> errs = e.getWriteErrors();
        for (BulkWriteError err : errs) {
          if (err.getCategory() != DUPLICATE_KEY) {
            throw handleMongoWriteError(e, err);
          }
        }
        BulkWriteResult res = e.getWriteResult();
        inserts.clear();
        res.getInserts().stream()
            .map(MongoDBPersist::objIdFromBulkWriteInsert)
            .mapToInt(id -> objIdIndex(objs, id))
            .mapToObj(docs::get)
            .forEach(inserts::add);
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
    }
  }

  private static ObjId objIdFromDoc(Document doc) {
    return binaryToObjId(doc.get(ID_PROPERTY_NAME, Document.class).get(COL_OBJ_ID, Binary.class));
  }

  private static int objIdIndex(Obj[] objs, ObjId id) {
    for (int i = 0; i < objs.length; i++) {
      if (id.equals(objs[i].id())) {
        return i;
      }
    }
    throw new IllegalArgumentException("ObjId " + id + " not in objs");
  }

  private static ObjId objIdFromBulkWriteInsert(BulkWriteInsert insert) {
    return ObjId.objIdFromByteArray(insert.getId().asDocument().getBinary(COL_OBJ_ID).getData());
  }

  private static ObjId objIdFromBulkWriteUpsert(BulkWriteUpsert upsert) {
    return ObjId.objIdFromByteArray(upsert.getId().asDocument().getBinary(COL_OBJ_ID).getData());
  }

  @Override
  public void deleteObj(@Nonnull ObjId id) {
    try {
      backend.objs().deleteOne(eq(ID_PROPERTY_NAME, idObjDoc(id)));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void deleteObjs(@Nonnull ObjId[] ids) {
    List<Document> list =
        Stream.of(ids).filter(Objects::nonNull).map(this::idObjDoc).collect(toList());
    if (list.isEmpty()) {
      return;
    }
    try {
      backend.objs().deleteMany(in(ID_PROPERTY_NAME, list));
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public void upsertObj(@Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    ReplaceOptions options = upsertOptions();

    long referenced = config.currentTimeMicros();
    Document doc = objToDoc(obj, referenced, false);
    UpdateResult result;
    try {
      result = backend.objs().replaceOne(eq(ID_PROPERTY_NAME, idObjDoc(id)), doc, options);
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
    if (!result.wasAcknowledged()) {
      throw new RuntimeException("Upsert not acknowledged");
    }
  }

  private static ReplaceOptions upsertOptions() {
    // A `ReplaceOneModel` with the default replace options (upsert==false) silently does just
    // nothing.
    ReplaceOptions options = new ReplaceOptions();
    options.upsert(true);
    return options;
  }

  @Override
  public void upsertObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
    ReplaceOptions options = upsertOptions();

    long referenced = config.currentTimeMicros();
    List<WriteModel<Document>> docs = new ArrayList<>(objs.length);
    for (Obj obj : objs) {
      if (obj != null) {
        ObjId id = obj.id();
        docs.add(
            new ReplaceOneModel<>(
                eq(ID_PROPERTY_NAME, idObjDoc(id)), objToDoc(obj, referenced, false), options));
      }
    }

    List<WriteModel<Document>> updates = new ArrayList<>(docs);
    if (!updates.isEmpty()) {
      BulkWriteResult res;
      try {
        res = backend.objs().bulkWrite(updates);
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
      if (!res.wasAcknowledged()) {
        throw new RuntimeException("Upsert not acknowledged");
      }
    }
  }

  @Override
  public boolean deleteWithReferenced(@Nonnull Obj obj) {
    ObjId id = obj.id();

    try {
      var referencedBson =
          obj.referenced() != -1L
              ? eq(COL_OBJ_REFERENCED, obj.referenced())
              : Filters.or(
                  eq(COL_OBJ_REFERENCED, 0L), Filters.not(Filters.exists(COL_OBJ_REFERENCED)));
      return backend
              .objs()
              .findOneAndDelete(and(eq(ID_PROPERTY_NAME, idObjDoc(id)), referencedBson))
          != null;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean deleteConditional(@Nonnull UpdateableObj obj) {
    ObjId id = obj.id();
    ObjType type = obj.type();

    try {
      return backend
              .objs()
              .findOneAndDelete(
                  and(
                      eq(ID_PROPERTY_NAME, idObjDoc(id)),
                      eq(COL_OBJ_TYPE, type.shortName()),
                      eq(COL_OBJ_VERS, obj.versionToken())))
          != null;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Override
  public boolean updateConditional(@Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue)
      throws ObjTooLargeException {
    ObjId id = expected.id();
    ObjType type = expected.type();
    String expectedVersion = expected.versionToken();

    checkArgument(id != null && id.equals(newValue.id()));
    checkArgument(expected.type().equals(newValue.type()));
    checkArgument(!expected.versionToken().equals(newValue.versionToken()));

    long referenced = config.currentTimeMicros();
    Document doc = objToDoc(newValue, referenced, false);

    List<Bson> updates =
        doc.entrySet().stream()
            .filter(e -> !ID_PROPERTY_NAME.equals(e.getKey()))
            .map(e -> set(e.getKey(), e.getValue()))
            .collect(toList());

    Bson update = Updates.combine(updates);

    try {
      return backend
              .objs()
              .findOneAndUpdate(
                  and(
                      eq(ID_PROPERTY_NAME, idObjDoc(id)),
                      eq(COL_OBJ_TYPE, type.shortName()),
                      eq(COL_OBJ_VERS, expectedVersion)),
                  update)
          != null;
    } catch (RuntimeException e) {
      throw unhandledException(e);
    }
  }

  @Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes);
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config.repositoryId()));
  }

  private <T extends Obj> T docToObj(Document doc, ObjType type, Class<T> typeClass) {
    ObjId id = objIdFromDoc(doc);
    return docToObj(id, doc, type, typeClass);
  }

  private <T extends Obj> T docToObj(
      @Nonnull ObjId id, Document doc, ObjType t, @SuppressWarnings("unused") Class<T> typeClass) {
    if (doc == null) {
      return null;
    }
    ObjType type = objTypeByName(doc.getString(COL_OBJ_TYPE));
    if (t != null && !t.equals(type)) {
      return null;
    }
    @SuppressWarnings("unchecked")
    ObjSerializer<T> serializer = (ObjSerializer<T>) ObjSerializers.forType(type);
    Document inner = doc.get(serializer.fieldName(), Document.class);
    String versionToken = doc.getString(COL_OBJ_VERS);
    Long referenced = doc.getLong(COL_OBJ_REFERENCED);
    return serializer.docToObj(
        id, type, referenced != null ? referenced : -1L, inner, versionToken);
  }

  private Document objToDoc(@Nonnull Obj obj, long referenced, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    ObjType type = obj.type();
    ObjSerializer<Obj> serializer = ObjSerializers.forType(type);

    Document doc = new Document();
    Document inner = new Document();
    doc.put(ID_PROPERTY_NAME, idObjDoc(id));
    doc.put(COL_OBJ_TYPE, type.shortName());
    var objReferenced = obj.referenced();
    if (objReferenced != -1L) {
      // -1 is a sentinel for AbstractBasePersistTests.deleteWithReferenced()
      doc.put(COL_OBJ_REFERENCED, referenced);
    }
    UpdateableObj.extractVersionToken(obj).ifPresent(token -> doc.put(COL_OBJ_VERS, token));
    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    serializer.objToDoc(obj, inner, incrementalIndexSizeLimit, indexSizeLimit);
    doc.put(serializer.fieldName(), inner);
    return doc;
  }

  private class ScanAllObjectsIterator extends AbstractIterator<Obj>
      implements CloseableIterator<Obj> {

    private final MongoCursor<Document> result;

    public ScanAllObjectsIterator(Set<ObjType> returnedObjTypes) {
      Bson condition = eq(ID_REPO_PATH, config.repositoryId());
      if (!returnedObjTypes.isEmpty()) {
        List<String> objTypeShortNames =
            returnedObjTypes.stream().map(ObjType::shortName).collect(toList());
        condition = and(condition, in(COL_OBJ_TYPE, objTypeShortNames));
      }
      try {
        result = backend.objs().find(condition).iterator();
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
    }

    @Override
    protected Obj computeNext() {
      if (!result.hasNext()) {
        return endOfData();
      }

      try {
        Document doc = result.next();
        return docToObj(doc, null, Obj.class);
      } catch (RuntimeException e) {
        throw unhandledException(e);
      }
    }

    @Override
    public void close() {
      result.close();
    }
  }

  static RuntimeException unhandledException(RuntimeException e) {
    if (e instanceof MongoInterruptedException
        || e instanceof MongoTimeoutException
        || e instanceof MongoServerUnavailableException
        || e instanceof MongoSocketReadTimeoutException
        || e instanceof MongoExecutionTimeoutException) {
      return new UnknownOperationResultException(e);
    }
    if (e instanceof MongoWriteException) {
      return handleMongoWriteException((MongoWriteException) e);
    }
    if (e instanceof MongoBulkWriteException) {
      MongoBulkWriteException specific = (MongoBulkWriteException) e;
      for (BulkWriteError error : specific.getWriteErrors()) {
        switch (error.getCategory()) {
          case EXECUTION_TIMEOUT:
          case UNCATEGORIZED:
            return new UnknownOperationResultException(e);
          default:
            break;
        }
      }
    }
    return e;
  }

  static RuntimeException handleMongoWriteException(MongoWriteException e) {
    return handleMongoWriteError(e, e.getError());
  }

  static RuntimeException handleMongoWriteError(MongoException e, WriteError error) {
    switch (error.getCategory()) {
      case EXECUTION_TIMEOUT:
      case UNCATEGORIZED:
        return new UnknownOperationResultException(e);
      default:
        return e;
    }
  }
}
