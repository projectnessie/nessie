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
package org.projectnessie.versioned.persist.mongodb;

import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.serialize.ProtoSerialization.toProto;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.ConnectionString;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;

public class MongoDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<MongoDatabaseAdapterConfig> {

  private static final String GLOBAL_POINTER = "global_pointer";
  private static final String GLOBAL_LOG = "global_log";
  private static final String COMMIT_LOG = "commit_log";
  private static final String KEY_LIST = "key_list";

  private final ByteString keyPrefix;
  private final byte[] globalPointerKey;

  private final MongoClient mongoClient;
  private final MongoCollection<Document> globalPointers;
  private final MongoCollection<Document> globalLog;
  private final MongoCollection<Document> commitLog;
  private final MongoCollection<Document> keyLists;

  protected MongoDatabaseAdapter(MongoDatabaseAdapterConfig config) {
    super(config);

    keyPrefix = ByteString.copyFromUtf8(config.getKeyPrefix());
    globalPointerKey = keyPrefix.toByteArray();

    ConnectionString cs = new ConnectionString(config.getConnectionString());
    MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(cs).build();

    mongoClient = MongoClients.create(settings);
    MongoDatabase database = mongoClient.getDatabase(config.getDatabaseName());
    globalPointers = database.getCollection(GLOBAL_POINTER);
    globalLog = database.getCollection(GLOBAL_LOG);
    commitLog = database.getCollection(COMMIT_LOG);
    keyLists = database.getCollection(KEY_LIST);
  }

  @Override
  public void reinitializeRepo(String defaultBranchName) {
    globalPointers.deleteMany(Filters.empty()); // empty filter matches all
    globalLog.deleteMany(Filters.empty());
    commitLog.deleteMany(Filters.empty());
    keyLists.deleteMany(Filters.empty());

    super.initializeRepo(defaultBranchName);
  }

  @Override
  public void close() {
    mongoClient.close();
  }

  private byte[] toId(Hash id) {
    return keyPrefix.concat(id.asBytes()).toByteArray();
  }

  private List<byte[]> toIds(Collection<Hash> ids) {
    return ids.stream().map(this::toId).collect(Collectors.toList());
  }

  private Document toDoc(Hash id, byte[] data) {
    return toDoc(toId(id), data);
  }

  private static Document toDoc(byte[] id, byte[] data) {
    Document doc = new Document();
    doc.put("_id", id);
    doc.put("data", data);
    return doc;
  }

  private Document toDoc(GlobalStatePointer pointer) {
    Document doc = new Document();
    doc.put("_id", globalPointerKey);
    doc.put("data", pointer.toByteArray());
    doc.put("globalId", pointer.getGlobalId().toByteArray());
    return doc;
  }

  private void insert(MongoCollection<Document> collection, Hash id, byte[] data)
      throws ReferenceConflictException {
    insert(collection, toDoc(id, data));
  }

  private static void insert(MongoCollection<Document> collection, Document doc)
      throws ReferenceConflictException {
    InsertOneResult result;
    try {
      result = collection.insertOne(doc);
    } catch (MongoWriteException writeException) {
      ErrorCategory category = writeException.getError().getCategory();
      if (ErrorCategory.DUPLICATE_KEY.equals(category)) {
        ReferenceConflictException ex = DatabaseAdapterUtil.hashCollisionDetected();
        ex.initCause(writeException);
        throw ex;
      }

      throw writeException;
    }

    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("Unacknowledged write to " + collection.getNamespace());
    }
  }

  private static void insert(MongoCollection<Document> collection, List<Document> docs)
      throws ReferenceConflictException {
    if (docs.isEmpty()) {
      return; // Mongo does not accept empty args to insertMany()
    }

    InsertManyResult result;
    try {
      result = collection.insertMany(docs);
    } catch (MongoWriteException writeException) {
      ErrorCategory category = writeException.getError().getCategory();
      if (ErrorCategory.DUPLICATE_KEY.equals(category)) {
        ReferenceConflictException ex = DatabaseAdapterUtil.hashCollisionDetected();
        ex.initCause(writeException);
        throw ex;
      }

      throw writeException;
    }

    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("Unacknowledged write to " + collection.getNamespace());
    }
  }

  private void delete(MongoCollection<Document> collection, Set<Hash> ids) {
    DeleteResult result = collection.deleteMany(Filters.in("_id", toIds(ids)));

    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("Unacknowledged write to " + collection.getNamespace());
    }
  }

  private byte[] loadById(MongoCollection<Document> collection, byte[] id) {
    Document doc = collection.find(Filters.eq(id)).first();
    if (doc == null) {
      return null;
    }

    Binary data = doc.get("data", Binary.class);
    if (data == null) {
      return null;
    }

    return data.getData();
  }

  private <T> T loadById(MongoCollection<Document> collection, Hash id, Parser<T> parser) {
    return loadById(collection, toId(id), parser);
  }

  private <T> T loadById(MongoCollection<Document> collection, byte[] id, Parser<T> parser) {
    byte[] data = loadById(collection, id);
    if (data == null) {
      return null;
    }

    try {
      return parser.parse(data);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Fetches a collection of documents by hash and returns them in the order of hashes requested.
   *
   * <p>Note: unknown hashes will correspond to {@code null} elements in the result list.
   */
  private <T> List<T> fetchMappedPage(
      MongoCollection<Document> collection, List<Hash> hashes, Function<Document, T> mapper) {
    List<byte[]> ids = hashes.stream().map(this::toId).collect(Collectors.toList());
    FindIterable<Document> docs = collection.find(Filters.in("_id", ids)).limit(hashes.size());

    HashMap<Hash, Document> loaded = new HashMap<>(hashes.size());
    for (Document doc : docs) {
      loaded.put(idAsHash(doc), doc);
    }

    List<T> result = new ArrayList<>(hashes.size());
    for (Hash hash : hashes) {
      T element = null;
      Document document = loaded.get(hash);
      if (document != null) {
        element = mapper.apply(document);
      }

      result.add(element); // nulls elements are permitted
    }

    return result;
  }

  private <T> List<T> fetchPage(
      MongoCollection<Document> collection, List<Hash> hashes, Parser<T> parser) {
    return fetchMappedPage(
        collection,
        hashes,
        document -> {
          try {
            byte[] data = data(document);
            return parser.parse(data);
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
          }
        });
  }

  @Override
  protected CommitLogEntry fetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return loadById(commitLog, hash, ProtoSerialization::protoToCommitLogEntry);
  }

  private static Hash idAsHash(Document doc) {
    byte[] id = doc.get("_id", Binary.class).getData();
    return Hash.of(ByteString.copyFrom(id));
  }

  private static byte[] data(Document doc) {
    return doc.get("data", Binary.class).getData();
  }

  @Override
  protected List<CommitLogEntry> fetchPageFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(commitLog, hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected int entitySize(KeyWithType entry) {
    return toProto(entry).getSerializedSize();
  }

  @Override
  protected Stream<KeyListEntity> fetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    return fetchMappedPage(
        keyLists,
        keyListsIds,
        document -> {
          Hash hash = idAsHash(document);
          KeyList keyList = protoToKeyList(data(document));
          return KeyListEntity.of(hash, keyList);
        })
        .stream();
  }

  @Override
  protected void writeIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    insert(commitLog, entry.getHash(), toProto(entry).toByteArray());
  }

  @Override
  protected void writeMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    List<Document> docs =
        entries.stream()
            .map(e -> toDoc(e.getHash(), toProto(e).toByteArray()))
            .collect(Collectors.toList());
    insert(commitLog, docs);
  }

  @Override
  protected void writeKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    for (KeyListEntity keyList : newKeyListEntities) {
      try {
        insert(keyLists, keyList.getId(), toProto(keyList.getKeys()).toByteArray());
      } catch (ReferenceConflictException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  protected void writeGlobalCommit(NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    insert(globalLog, toDoc(entry.getId().toByteArray(), entry.toByteArray()));
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    Document doc = toDoc(pointer);

    UpdateResult result =
        globalPointers.updateOne(
            Filters.eq((Object) globalPointerKey),
            new Document("$set", doc),
            new UpdateOptions().upsert(true));

    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("Unacknowledged write to " + globalPointers.getNamespace());
    }
  }

  @Override
  protected boolean globalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    Document doc = toDoc(newPointer);
    byte[] expectedGlobalId = expected.getGlobalId().toByteArray();

    UpdateResult result =
        globalPointers.updateOne(
            Filters.and(Filters.eq(globalPointerKey), Filters.eq("globalId", expectedGlobalId)),
            new Document("$set", doc));

    return result.wasAcknowledged()
        && result.getMatchedCount() == 1
        && result.getModifiedCount() == 1;
  }

  @Override
  protected void cleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists) {
    globalLog.deleteOne(Filters.eq(toId(globalId)));

    delete(commitLog, branchCommits);
    delete(keyLists, newKeyLists);
  }

  @Override
  protected GlobalStatePointer fetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return loadById(globalPointers, globalPointerKey, GlobalStatePointer::parseFrom);
  }

  @Override
  protected GlobalStateLogEntry fetchFromGlobalLog(NonTransactionalOperationContext ctx, Hash id) {
    return loadById(globalLog, id, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected List<GlobalStateLogEntry> fetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(globalLog, hashes, GlobalStateLogEntry::parseFrom);
  }

  @FunctionalInterface
  private interface Parser<T> {
    T parse(byte[] data) throws InvalidProtocolBufferException;
  }
}
