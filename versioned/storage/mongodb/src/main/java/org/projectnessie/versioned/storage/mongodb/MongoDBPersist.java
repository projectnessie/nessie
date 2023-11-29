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
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_TYPE;
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
import com.mongodb.MongoWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
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
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
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
  @jakarta.annotation.Nonnull
  @Override
  public String name() {
    return MongoDBBackendFactory.NAME;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
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
  @jakarta.annotation.Nonnull
  @Override
  public Reference addReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefAlreadyExistsException {
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
      throw e;
    }
    return reference;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Reference markReferenceAsDeleted(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(false);
    UpdateResult result =
        backend.refs().updateOne(referenceCondition(reference), set(COL_REFERENCES_DELETED, true));
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
  @jakarta.annotation.Nonnull
  @Override
  public Reference updateReferencePointer(
      @Nonnull @jakarta.annotation.Nonnull Reference reference,
      @Nonnull @jakarta.annotation.Nonnull ObjId newPointer)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(false);

    Reference updated = reference.forNewPointer(newPointer, config);
    List<Bson> updates = new ArrayList<>();
    updates.add(set(COL_REFERENCES_POINTER, objIdToBinary(newPointer)));
    byte[] previous = serializePreviousPointers(updated.previousPointers());
    if (previous != null) {
      updates.add(set(COL_REFERENCES_PREVIOUS, new Binary(previous)));
    }

    UpdateResult result = backend.refs().updateOne(referenceCondition(reference), updates);
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
  public void purgeReference(@Nonnull @jakarta.annotation.Nonnull Reference reference)
      throws RefNotFoundException, RefConditionFailedException {
    reference = reference.withDeleted(true);
    DeleteResult result = backend.refs().deleteOne(referenceCondition(reference));
    if (result.getDeletedCount() != 1) {
      Reference ex = fetchReference(reference.name());
      if (ex == null) {
        throw new RefNotFoundException(reference.name());
      }
      throw new RefConditionFailedException(ex);
    }
  }

  @Override
  public Reference fetchReference(@Nonnull @jakarta.annotation.Nonnull String name) {
    FindIterable<Document> result = backend.refs().find(eq(ID_PROPERTY_NAME, idRefDoc(name)));

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
  @jakarta.annotation.Nonnull
  @Override
  public Reference[] fetchReferences(@Nonnull @jakarta.annotation.Nonnull String[] names) {
    List<Document> nameIdDocs =
        Arrays.stream(names).filter(Objects::nonNull).map(this::idRefDoc).collect(toList());
    FindIterable<Document> result = backend.refs().find(in(ID_PROPERTY_NAME, nameIdDocs));
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
  @jakarta.annotation.Nonnull
  public Obj fetchObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) throws ObjNotFoundException {
    FindIterable<Document> result = backend.objs().find(eq(ID_PROPERTY_NAME, idObjDoc(id)));

    Document doc = result.first();
    if (doc == null) {
      throw new ObjNotFoundException(id);
    }

    return docToObj(id, doc);
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public <T extends Obj> T fetchTypedObj(
      @Nonnull @jakarta.annotation.Nonnull ObjId id, ObjType type, Class<T> typeClass)
      throws ObjNotFoundException {
    FindIterable<Document> result =
        backend
            .objs()
            .find(and(eq(ID_PROPERTY_NAME, idObjDoc(id)), eq(COL_OBJ_TYPE, type.shortName())));

    Document doc = result.first();
    if (doc == null) {
      throw new ObjNotFoundException(id);
    }

    Obj obj = docToObj(id, doc);

    @SuppressWarnings("unchecked")
    T r = (T) obj;
    return r;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public ObjType fetchObjType(@Nonnull @jakarta.annotation.Nonnull ObjId id)
      throws ObjNotFoundException {
    FindIterable<Document> result = backend.objs().find(eq(ID_PROPERTY_NAME, idObjDoc(id)));

    Document doc = result.first();
    if (doc == null) {
      throw new ObjNotFoundException(id);
    }

    return ObjTypes.forShortName(doc.getString(COL_OBJ_TYPE));
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public Obj[] fetchObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids)
      throws ObjNotFoundException {
    List<Document> list = new ArrayList<>(ids.length);
    Object2IntHashMap<ObjId> idToIndex =
        new Object2IntHashMap<>(ids.length * 2, Hashing.DEFAULT_LOAD_FACTOR, -1);
    Obj[] r = new Obj[ids.length];
    for (int i = 0; i < ids.length; i++) {
      ObjId id = ids[i];
      if (id != null) {
        list.add(idObjDoc(id));
        idToIndex.put(id, i);
      }
    }

    if (!list.isEmpty()) {
      fetchObjsPage(r, list, idToIndex);
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

  private void fetchObjsPage(Obj[] r, List<Document> list, Object2IntHashMap<ObjId> idToIndex) {
    FindIterable<Document> result = backend.objs().find(in(ID_PROPERTY_NAME, list));
    for (Document doc : result) {
      Obj obj = docToObj(doc);
      int idx = idToIndex.getValue(obj.id());
      if (idx != -1) {
        r[idx] = obj;
      }
    }
  }

  @Override
  public boolean storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    Document doc = objToDoc(obj, ignoreSoftSizeRestrictions);
    try {
      backend.objs().insertOne(doc);
    } catch (MongoWriteException e) {
      if (e.getError().getCategory() == DUPLICATE_KEY) {
        return false;
      }
      throw e;
    }

    return true;
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public boolean[] storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    List<WriteModel<Document>> docs = new ArrayList<>(objs.length);
    for (Obj obj : objs) {
      if (obj != null) {
        docs.add(new InsertOneModel<>(objToDoc(obj, false)));
      }
    }

    boolean[] r = new boolean[objs.length];

    List<WriteModel<Document>> inserts = new ArrayList<>(docs);
    while (!inserts.isEmpty()) {
      try {
        BulkWriteResult res = backend.objs().bulkWrite(inserts);
        for (BulkWriteInsert insert : res.getInserts()) {
          ObjId id = objIdFromBulkWriteInsert(insert);
          r[objIdIndex(objs, id)] = id != null;
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
            throw e;
          }
        }
        BulkWriteResult res = e.getWriteResult();
        inserts.clear();
        res.getInserts().stream()
            .map(MongoDBPersist::objIdFromBulkWriteInsert)
            .mapToInt(id -> objIdIndex(objs, id))
            .mapToObj(docs::get)
            .forEach(inserts::add);
      }
    }
    return r;
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

  @Override
  public void deleteObj(@Nonnull @jakarta.annotation.Nonnull ObjId id) {
    backend.objs().deleteOne(eq(ID_PROPERTY_NAME, idObjDoc(id)));
  }

  @Override
  public void deleteObjs(@Nonnull @jakarta.annotation.Nonnull ObjId[] ids) {
    List<Document> list =
        Stream.of(ids).filter(Objects::nonNull).map(this::idObjDoc).collect(toList());
    if (list.isEmpty()) {
      return;
    }
    backend.objs().deleteMany(in(ID_PROPERTY_NAME, list));
  }

  @Override
  public void upsertObj(@Nonnull @jakarta.annotation.Nonnull Obj obj) throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    ReplaceOptions options = upsertOptions();

    Document doc = objToDoc(obj, false);
    UpdateResult result =
        backend.objs().replaceOne(eq(ID_PROPERTY_NAME, idObjDoc(id)), doc, options);
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
  public void upsertObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
      throws ObjTooLargeException {
    ReplaceOptions options = upsertOptions();

    List<WriteModel<Document>> docs = new ArrayList<>(objs.length);
    for (Obj obj : objs) {
      if (obj != null) {
        ObjId id = obj.id();
        docs.add(
            new ReplaceOneModel<>(
                eq(ID_PROPERTY_NAME, idObjDoc(id)), objToDoc(obj, false), options));
      }
    }

    List<WriteModel<Document>> updates = new ArrayList<>(docs);
    if (!updates.isEmpty()) {
      BulkWriteResult res = backend.objs().bulkWrite(updates);
      if (!res.wasAcknowledged()) {
        throw new RuntimeException("Upsert not acknowledged");
      }
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  @Override
  public CloseableIterator<Obj> scanAllObjects(
      @Nonnull @jakarta.annotation.Nonnull Set<ObjType> returnedObjTypes) {
    return new ScanAllObjectsIterator(returnedObjTypes);
  }

  @Override
  public void erase() {
    backend.eraseRepositories(singleton(config.repositoryId()));
  }

  private Obj docToObj(Document doc) {
    ObjId id = objIdFromDoc(doc);
    return docToObj(id, doc);
  }

  private Obj docToObj(@Nonnull @jakarta.annotation.Nonnull ObjId id, Document doc) {
    ObjType type = ObjTypes.forShortName(doc.getString(COL_OBJ_TYPE));
    ObjSerializer<?> serializer = ObjSerializers.forType(type);
    Document inner = doc.get(serializer.fieldName(), Document.class);
    return serializer.docToObj(id, inner);
  }

  private Document objToDoc(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    ObjType type = obj.type();
    ObjSerializer<Obj> serializer = ObjSerializers.forType(type);

    Document doc = new Document();
    Document inner = new Document();
    doc.put(ID_PROPERTY_NAME, idObjDoc(id));
    doc.put(COL_OBJ_TYPE, type.shortName());
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
      List<String> objTypeShortNames =
          returnedObjTypes.stream().map(ObjType::shortName).collect(toList());
      result =
          backend
              .objs()
              .find(
                  and(eq(ID_REPO_PATH, config.repositoryId()), in(COL_OBJ_TYPE, objTypeShortNames)))
              .iterator();
    }

    @Override
    protected Obj computeNext() {
      if (!result.hasNext()) {
        return endOfData();
      }

      Document doc = result.next();
      return docToObj(doc);
    }

    @Override
    public void close() {
      result.close();
    }
  }
}
