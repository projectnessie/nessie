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
import static com.google.common.base.Preconditions.checkState;
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
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_CREATED;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_INCOMPLETE_INDEX;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_INCREMENTAL_INDEX;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_MESSAGE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_REFERENCE_INDEX;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_REFERENCE_INDEX_STRIPES;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_SECONDARY_PARENTS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_SEQ;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_TAIL;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_COMMIT_TYPE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_INDEX;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_INDEX_INDEX;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_ID;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_OBJ_TYPE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REF;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_CREATED_AT;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_DELETED;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_NAME;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_POINTER;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REFERENCES_PREVIOUS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REF_CREATED_AT;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REF_EXTENDED_INFO;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REF_INITIAL_POINTER;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REF_NAME;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_REPO;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_SEGMENTS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_SEGMENTS_STRIPES;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRING;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRING_COMPRESSION;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRING_CONTENT_TYPE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRING_FILENAME;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRING_PREDECESSORS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRING_TEXT;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRIPES_FIRST_KEY;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRIPES_LAST_KEY;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_STRIPES_SEGMENT;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_TAG;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_TAG_HEADERS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_TAG_MESSAGE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_TAG_SIGNATURE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_VALUE;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_VALUE_CONTENT_ID;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_VALUE_DATA;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.COL_VALUE_PAYLOAD;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.ID_PROPERTY_NAME;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.ID_REPO_PATH;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;

import com.google.common.collect.AbstractIterator;
import com.mongodb.MongoWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2IntHashMap;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
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

public class MongoDBPersist implements Persist {
  private static final Map<ObjType, StoreObjDesc<?>> STORE_OBJ_TYPE = new EnumMap<>(ObjType.class);
  private static final ObjType[] ALL_OBJ_TYPES = ObjType.values();

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

  private static Binary bytesToBinary(ByteString bytes) {
    return new Binary(bytes.toByteArray());
  }

  private static ByteString binaryToBytes(Binary binary) {
    return binary != null ? unsafeWrap(binary.getData()) : null;
  }

  private static Binary objIdToBinary(ObjId id) {
    return new Binary(id.asByteArray());
  }

  private static ObjId binaryToObjId(Binary id) {
    return id != null ? ObjId.objIdFromByteArray(id.getData()) : null;
  }

  private static void objIdsToDoc(Document doc, String n, List<ObjId> ids) {
    if (ids == null || ids.isEmpty()) {
      return;
    }
    doc.put(n, objIdsToBinary(ids));
  }

  private static List<Binary> objIdsToBinary(List<ObjId> ids) {
    if (ids == null) {
      return emptyList();
    }
    return ids.stream().map(MongoDBPersist::objIdToBinary).collect(toList());
  }

  private static List<ObjId> binaryToObjIds(List<Binary> ids) {
    if (ids == null) {
      return emptyList();
    }
    return ids.stream().map(MongoDBPersist::binaryToObjId).collect(toList());
  }

  private static void binaryToObjIds(List<Binary> ids, Consumer<ObjId> receiver) {
    if (ids != null) {
      ids.stream().map(MongoDBPersist::binaryToObjId).forEach(receiver);
    }
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

    return objTypeFromItem(doc);
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

  private static ObjId objIdFromDoc(Document doc) {
    return binaryToObjId(doc.get(ID_PROPERTY_NAME, Document.class).get(COL_OBJ_ID, Binary.class));
  }

  private ObjType objTypeFromItem(Document doc) {
    return objTypeFromItem(doc.getString(COL_OBJ_TYPE));
  }

  private ObjType objTypeFromItem(String shortType) {
    for (ObjType type : ALL_OBJ_TYPES) {
      if (type.shortName().equals(shortType)) {
        return type;
      }
    }
    throw new IllegalStateException("Cannot deserialize object short type " + shortType);
  }

  private StoreObjDesc<?> objTypeFromDoc(Document doc) {
    ObjType type = objTypeFromItem(doc);
    StoreObjDesc<?> storeObj = STORE_OBJ_TYPE.get(type);
    checkState(storeObj != null, "Cannot deserialize object type %s", type);
    return storeObj;
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
  public void storeObj(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    ReplaceOptions options = upsertOptions();

    Document doc = objToDoc(obj, ignoreSoftSizeRestrictions);
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
  public void storeObjs(@Nonnull @jakarta.annotation.Nonnull Obj[] objs)
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
    StoreObjDesc<?> storeObj = objTypeFromDoc(doc);
    Document inner = doc.get(storeObj.typeName, Document.class);
    return storeObj.docToObj(id, inner);
  }

  @SuppressWarnings("unchecked")
  private Document objToDoc(
      @Nonnull @jakarta.annotation.Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
      throws ObjTooLargeException {
    ObjId id = obj.id();
    checkArgument(id != null, "Obj to store must have a non-null ID");

    ObjType type = obj.type();
    @SuppressWarnings("rawtypes")
    StoreObjDesc storeObj = STORE_OBJ_TYPE.get(type);
    checkArgument(storeObj != null, "Cannot serialize object type %s ", type);

    Document doc = new Document();
    Document inner = new Document();
    doc.put(ID_PROPERTY_NAME, idObjDoc(id));
    doc.put(COL_OBJ_TYPE, type.shortName());
    int incrementalIndexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIncrementalIndexSizeLimit();
    int indexSizeLimit =
        ignoreSoftSizeRestrictions ? Integer.MAX_VALUE : effectiveIndexSegmentSizeLimit();
    storeObj.objToDoc(obj, inner, incrementalIndexSizeLimit, indexSizeLimit);
    doc.put(storeObj.typeName, inner);
    return doc;
  }

  abstract static class StoreObjDesc<O extends Obj> {
    final String typeName;

    StoreObjDesc(String typeName) {
      this.typeName = typeName;
    }

    abstract void objToDoc(
        O obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize)
        throws ObjTooLargeException;

    abstract O docToObj(ObjId id, Document doc);
  }

  static {
    STORE_OBJ_TYPE.put(
        ObjType.COMMIT,
        new StoreObjDesc<CommitObj>(COL_COMMIT) {
          @Override
          void objToDoc(
              CommitObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize)
              throws ObjTooLargeException {
            doc.put(COL_COMMIT_SEQ, obj.seq());
            doc.put(COL_COMMIT_CREATED, obj.created());
            ObjId referenceIndex = obj.referenceIndex();
            if (referenceIndex != null) {
              doc.put(COL_COMMIT_REFERENCE_INDEX, objIdToBinary(referenceIndex));
            }
            doc.put(COL_COMMIT_MESSAGE, obj.message());
            objIdsToDoc(doc, COL_COMMIT_TAIL, obj.tail());
            objIdsToDoc(doc, COL_COMMIT_SECONDARY_PARENTS, obj.secondaryParents());

            ByteString index = obj.incrementalIndex();
            if (index.size() > incrementalIndexLimit) {
              throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
            }
            doc.put(COL_COMMIT_INCREMENTAL_INDEX, bytesToBinary(index));

            List<IndexStripe> indexStripes = obj.referenceIndexStripes();
            if (!indexStripes.isEmpty()) {
              doc.put(COL_COMMIT_REFERENCE_INDEX_STRIPES, stripesToDocs(indexStripes));
            }

            Document headerDoc = new Document();
            CommitHeaders headers = obj.headers();
            for (String s : headers.keySet()) {
              headerDoc.put(s, headers.getAll(s));
            }
            if (!headerDoc.isEmpty()) {
              doc.put(COL_COMMIT_HEADERS, headerDoc);
            }

            doc.put(COL_COMMIT_INCOMPLETE_INDEX, obj.incompleteIndex());
            doc.put(COL_COMMIT_TYPE, obj.commitType().shortName());
          }

          @Override
          CommitObj docToObj(ObjId id, Document doc) {
            CommitObj.Builder b =
                commitBuilder()
                    .id(id)
                    .seq(doc.getLong(COL_COMMIT_SEQ))
                    .created(doc.getLong(COL_COMMIT_CREATED))
                    .message(doc.getString(COL_COMMIT_MESSAGE))
                    .incrementalIndex(
                        binaryToBytes(doc.get(COL_COMMIT_INCREMENTAL_INDEX, Binary.class)))
                    .incompleteIndex(doc.getBoolean(COL_COMMIT_INCOMPLETE_INDEX))
                    .commitType(CommitType.fromShortName(doc.getString(COL_COMMIT_TYPE)));
            Binary v = doc.get(COL_COMMIT_REFERENCE_INDEX, Binary.class);
            if (v != null) {
              b.referenceIndex(binaryToObjId(v));
            }

            fromStripesDocList(
                doc, COL_COMMIT_REFERENCE_INDEX_STRIPES, b::addReferenceIndexStripes);

            binaryToObjIds(doc.getList(COL_COMMIT_TAIL, Binary.class), b::addTail);
            binaryToObjIds(
                doc.getList(COL_COMMIT_SECONDARY_PARENTS, Binary.class), b::addSecondaryParents);

            CommitHeaders.Builder headers = newCommitHeaders();
            Document headerDoc = doc.get(COL_COMMIT_HEADERS, Document.class);
            if (headerDoc != null) {
              headerDoc.forEach(
                  (k, o) -> {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    List<String> l = (List) o;
                    l.forEach(hv -> headers.add(k, hv));
                  });
            }
            b.headers(headers.build());

            return b.build();
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.REF,
        new StoreObjDesc<RefObj>(COL_REF) {

          @Override
          void objToDoc(
              RefObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
            doc.put(COL_REF_NAME, obj.name());
            doc.put(COL_REF_CREATED_AT, obj.createdAtMicros());
            doc.put(COL_REF_INITIAL_POINTER, objIdToBinary(obj.initialPointer()));
            ObjId extendedInfoObj = obj.extendedInfoObj();
            if (extendedInfoObj != null) {
              doc.put(COL_REF_EXTENDED_INFO, objIdToBinary(extendedInfoObj));
            }
          }

          @Override
          RefObj docToObj(ObjId id, Document doc) {
            return ref(
                id,
                doc.getString(COL_REF_NAME),
                binaryToObjId(doc.get(COL_REF_INITIAL_POINTER, Binary.class)),
                doc.getLong(COL_REF_CREATED_AT),
                binaryToObjId(doc.get(COL_REF_EXTENDED_INFO, Binary.class)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.VALUE,
        new StoreObjDesc<ContentValueObj>(COL_VALUE) {
          @Override
          void objToDoc(
              ContentValueObj obj,
              Document doc,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            doc.put(COL_VALUE_CONTENT_ID, obj.contentId());
            doc.put(COL_VALUE_PAYLOAD, obj.payload());
            doc.put(COL_VALUE_DATA, bytesToBinary(obj.data()));
          }

          @Override
          ContentValueObj docToObj(ObjId id, Document doc) {
            return contentValue(
                id,
                doc.getString(COL_VALUE_CONTENT_ID),
                doc.getInteger(COL_VALUE_PAYLOAD),
                binaryToBytes(doc.get(COL_VALUE_DATA, Binary.class)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.INDEX_SEGMENTS,
        new StoreObjDesc<IndexSegmentsObj>(COL_SEGMENTS) {
          @Override
          void objToDoc(
              IndexSegmentsObj obj,
              Document doc,
              int incrementalIndexLimit,
              int maxSerializedIndexSize) {
            doc.put(COL_SEGMENTS_STRIPES, stripesToDocs(obj.stripes()));
          }

          @Override
          IndexSegmentsObj docToObj(ObjId id, Document doc) {
            List<IndexStripe> stripes = new ArrayList<>();
            fromStripesDocList(doc, COL_SEGMENTS_STRIPES, stripes::add);
            return indexSegments(id, stripes);
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.INDEX,
        new StoreObjDesc<IndexObj>(COL_INDEX) {
          @Override
          void objToDoc(
              IndexObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize)
              throws ObjTooLargeException {
            ByteString index = obj.index();
            if (index.size() > maxSerializedIndexSize) {
              throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
            }
            doc.put(COL_INDEX_INDEX, bytesToBinary(index));
          }

          @Override
          IndexObj docToObj(ObjId id, Document doc) {
            return index(id, binaryToBytes(doc.get(COL_INDEX_INDEX, Binary.class)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.TAG,
        new StoreObjDesc<TagObj>(COL_TAG) {
          @Override
          void objToDoc(
              TagObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
            String message = obj.message();
            if (message != null) {
              doc.put(COL_TAG_MESSAGE, message);
            }

            Document headerDoc = new Document();
            CommitHeaders headers = obj.headers();
            if (headers != null) {
              for (String s : headers.keySet()) {
                headerDoc.put(s, headers.getAll(s));
              }
              if (!headerDoc.isEmpty()) {
                doc.put(COL_TAG_HEADERS, headerDoc);
              }
            }

            ByteString signature = obj.signature();
            if (signature != null) {
              doc.put(COL_TAG_SIGNATURE, bytesToBinary(signature));
            }
          }

          @Override
          TagObj docToObj(ObjId id, Document doc) {
            CommitHeaders tagHeaders = null;
            Document headerDoc = doc.get(COL_COMMIT_HEADERS, Document.class);
            if (headerDoc != null) {
              CommitHeaders.Builder headers = newCommitHeaders();
              headerDoc.forEach(
                  (k, o) -> {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    List<String> l = (List) o;
                    l.forEach(hv -> headers.add(k, hv));
                  });
              tagHeaders = headers.build();
            }

            return tag(
                id,
                doc.getString(COL_TAG_MESSAGE),
                tagHeaders,
                binaryToBytes(doc.get(COL_TAG_SIGNATURE, Binary.class)));
          }
        });
    STORE_OBJ_TYPE.put(
        ObjType.STRING,
        new StoreObjDesc<StringObj>(COL_STRING) {
          @Override
          void objToDoc(
              StringObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
            String s = obj.contentType();
            if (s != null && !s.isEmpty()) {
              doc.put(COL_STRING_CONTENT_TYPE, s);
            }
            doc.put(COL_STRING_COMPRESSION, obj.compression().name());
            s = obj.filename();
            if (s != null && !s.isEmpty()) {
              doc.put(COL_STRING_FILENAME, s);
            }
            objIdsToDoc(doc, COL_STRING_PREDECESSORS, obj.predecessors());
            doc.put(COL_STRING_TEXT, bytesToBinary(obj.text()));
          }

          @Override
          StringObj docToObj(ObjId id, Document doc) {
            return stringData(
                id,
                doc.getString(COL_STRING_CONTENT_TYPE),
                Compression.valueOf(doc.getString(COL_STRING_COMPRESSION)),
                doc.getString(COL_STRING_FILENAME),
                binaryToObjIds(doc.getList(COL_STRING_PREDECESSORS, Binary.class)),
                binaryToBytes(doc.get(COL_STRING_TEXT, Binary.class)));
          }
        });
  }

  private static void fromStripesDocList(
      Document doc, String attrName, Consumer<IndexStripe> consumer) {
    List<Document> refIndexStripes = doc.getList(attrName, Document.class);
    if (refIndexStripes != null) {
      for (Document seg : refIndexStripes) {
        consumer.accept(
            indexStripe(
                keyFromString(seg.getString(COL_STRIPES_FIRST_KEY)),
                keyFromString(seg.getString(COL_STRIPES_LAST_KEY)),
                binaryToObjId(seg.get(COL_STRIPES_SEGMENT, Binary.class))));
      }
    }
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  private static List<Document> stripesToDocs(List<IndexStripe> stripes) {
    List<Document> stripesDocs = new ArrayList<>();
    for (IndexStripe stripe : stripes) {
      Document sv = new Document();
      sv.put(COL_STRIPES_FIRST_KEY, stripe.firstKey().rawString());
      sv.put(COL_STRIPES_LAST_KEY, stripe.lastKey().rawString());
      sv.put(COL_STRIPES_SEGMENT, objIdToBinary(stripe.segment()));
      stripesDocs.add(sv);
    }
    return stripesDocs;
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
