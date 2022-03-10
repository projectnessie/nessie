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

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentVariantSupplier;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyWithType;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RepoProps;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.serialize.ProtoSerialization.Parser;

public class MongoDatabaseAdapter
    extends NonTransactionalDatabaseAdapter<NonTransactionalDatabaseAdapterConfig> {

  private static final String ID_PROPERTY_NAME = "_id";
  private static final String ID_REPO_NAME = "repo";
  private static final String ID_HASH_NAME = "hash";
  private static final String ID_REPO_PATH = ID_PROPERTY_NAME + "." + ID_REPO_NAME;
  private static final String DATA_PROPERTY_NAME = "data";
  private static final String GLOBAL_ID_PROPERTY_NAME = "globalId";

  private final String repositoryId;
  private final String globalPointerKey;

  private final MongoDatabaseClient client;

  protected MongoDatabaseAdapter(
      NonTransactionalDatabaseAdapterConfig config,
      MongoDatabaseClient client,
      ContentVariantSupplier contentVariantSupplier) {
    super(config, contentVariantSupplier);

    Objects.requireNonNull(client, "MongoDatabaseClient cannot be null");
    this.client = client;

    this.repositoryId = config.getRepositoryId();
    Objects.requireNonNull(repositoryId, "Repository ID cannot be null");

    globalPointerKey = repositoryId;
  }

  @Override
  public void eraseRepo() {
    client.getGlobalPointers().deleteMany(Filters.eq(globalPointerKey));
    Bson idPrefixFilter = Filters.eq(ID_REPO_PATH, repositoryId);
    client.getGlobalLog().deleteMany(idPrefixFilter);
    client.getCommitLog().deleteMany(idPrefixFilter);
    client.getKeyLists().deleteMany(idPrefixFilter);
    client.getRefLog().deleteMany(idPrefixFilter);
  }

  private Document toId(Hash id) {
    Document idDoc = new Document();
    // Note: the order of `put` calls matters
    idDoc.put(ID_REPO_NAME, repositoryId);
    idDoc.put(ID_HASH_NAME, id.asString());
    return idDoc;
  }

  private List<Document> toIds(Collection<Hash> ids) {
    return ids.stream().map(this::toId).collect(Collectors.toList());
  }

  private Document toDoc(Hash id, byte[] data) {
    return toDoc(toId(id), data);
  }

  private static Document toDoc(Document id, byte[] data) {
    Document doc = new Document();
    doc.put(ID_PROPERTY_NAME, id);
    doc.put(DATA_PROPERTY_NAME, data);
    return doc;
  }

  private Document toDoc(GlobalStatePointer pointer) {
    Document doc = new Document();
    doc.put(ID_PROPERTY_NAME, globalPointerKey);
    doc.put(DATA_PROPERTY_NAME, pointer.toByteArray());
    doc.put(GLOBAL_ID_PROPERTY_NAME, pointer.getGlobalId().toByteArray());
    return doc;
  }

  private Document toDoc(RepoProps pointer) {
    Document doc = new Document();
    doc.put(ID_PROPERTY_NAME, globalPointerKey);
    doc.put(DATA_PROPERTY_NAME, pointer.toByteArray());
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

  private void delete(MongoCollection<Document> collection, Collection<Hash> ids) {
    DeleteResult result = collection.deleteMany(Filters.in(ID_PROPERTY_NAME, toIds(ids)));

    if (!result.wasAcknowledged()) {
      throw new IllegalStateException("Unacknowledged write to " + collection.getNamespace());
    }
  }

  private <ID> byte[] loadById(MongoCollection<Document> collection, ID id) {
    Document doc = collection.find(Filters.eq(id)).first();
    if (doc == null) {
      return null;
    }

    Binary data = doc.get(DATA_PROPERTY_NAME, Binary.class);
    if (data == null) {
      return null;
    }

    return data.getData();
  }

  private <T> T loadById(MongoCollection<Document> collection, Hash id, Parser<T> parser) {
    return loadById(collection, toId(id), parser);
  }

  private <T, ID> T loadById(MongoCollection<Document> collection, ID id, Parser<T> parser) {
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
    List<Document> ids = hashes.stream().map(this::toId).collect(Collectors.toList());
    FindIterable<Document> docs =
        collection.find(Filters.in(ID_PROPERTY_NAME, ids)).limit(hashes.size());

    HashMap<Hash, Document> loaded = new HashMap<>(hashes.size() * 4 / 3 + 1, 0.75f);
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
  protected CommitLogEntry doFetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    return loadById(client.getCommitLog(), hash, ProtoSerialization::protoToCommitLogEntry);
  }

  private Hash idAsHash(Document doc) {
    Document id = doc.get(ID_PROPERTY_NAME, Document.class);

    String repo = id.getString(ID_REPO_NAME);
    if (!repositoryId.equals(repo)) {
      throw new IllegalStateException(
          String.format(
              "Repository mismatch for id '%s' (expected repository ID: '%s')", id, repositoryId));
    }

    String hash = id.getString(ID_HASH_NAME);
    return Hash.of(hash);
  }

  private static byte[] data(Document doc) {
    return doc.get(DATA_PROPERTY_NAME, Binary.class).getData();
  }

  @Override
  protected List<CommitLogEntry> doFetchMultipleFromCommitLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(client.getCommitLog(), hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected RepoDescription doFetchRepositoryDescription(NonTransactionalOperationContext ctx) {
    return loadById(
        client.getRepoDesc(), globalPointerKey, ProtoSerialization::protoToRepoDescription);
  }

  @Override
  protected boolean doTryUpdateRepositoryDescription(
      NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo) {
    Document doc = toDoc(toProto(updateTo));

    if (expected != null) {
      byte[] expectedBytes = toProto(expected).toByteArray();

      UpdateResult result =
          client
              .getRepoDesc()
              .replaceOne(
                  Filters.and(
                      Filters.eq(globalPointerKey), Filters.eq(DATA_PROPERTY_NAME, expectedBytes)),
                  doc);
      return result.wasAcknowledged()
          && result.getMatchedCount() == 1
          && result.getModifiedCount() == 1;
    } else {
      InsertOneResult result = client.getRepoDesc().insertOne(doc);
      return result.wasAcknowledged();
    }
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
  protected Stream<KeyListEntity> doFetchKeyLists(
      NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    return fetchMappedPage(
        client.getKeyLists(),
        keyListsIds,
        document -> {
          Hash hash = idAsHash(document);
          KeyList keyList = protoToKeyList(data(document));
          return KeyListEntity.of(hash, keyList);
        })
        .stream();
  }

  @Override
  protected void doWriteIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
      throws ReferenceConflictException {
    insert(client.getCommitLog(), entry.getHash(), toProto(entry).toByteArray());
  }

  @Override
  protected void doWriteMultipleCommits(
      NonTransactionalOperationContext ctx, List<CommitLogEntry> entries)
      throws ReferenceConflictException {
    List<Document> docs =
        entries.stream()
            .map(e -> toDoc(e.getHash(), toProto(e).toByteArray()))
            .collect(Collectors.toList());
    insert(client.getCommitLog(), docs);
  }

  @Override
  protected void doWriteKeyListEntities(
      NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    for (KeyListEntity keyList : newKeyListEntities) {
      try {
        insert(client.getKeyLists(), keyList.getId(), toProto(keyList.getKeys()).toByteArray());
      } catch (ReferenceConflictException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  protected void doWriteGlobalCommit(
      NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
      throws ReferenceConflictException {
    Document id = toId(Hash.of(entry.getId()));
    insert(client.getGlobalLog(), toDoc(id, entry.toByteArray()));
  }

  @Override
  protected void unsafeWriteGlobalPointer(
      NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    Document doc = toDoc(pointer);

    UpdateResult result =
        client
            .getGlobalPointers()
            .updateOne(
                Filters.eq((Object) globalPointerKey),
                new Document("$set", doc),
                new UpdateOptions().upsert(true));

    if (!result.wasAcknowledged()) {
      throw new IllegalStateException(
          "Unacknowledged write to " + client.getGlobalPointers().getNamespace());
    }
  }

  @Override
  protected boolean doGlobalPointerCas(
      NonTransactionalOperationContext ctx,
      GlobalStatePointer expected,
      GlobalStatePointer newPointer) {
    Document doc = toDoc(newPointer);
    byte[] expectedGlobalId = expected.getGlobalId().toByteArray();

    UpdateResult result =
        client
            .getGlobalPointers()
            .replaceOne(
                Filters.and(
                    Filters.eq(globalPointerKey),
                    Filters.eq(GLOBAL_ID_PROPERTY_NAME, expectedGlobalId)),
                doc);

    return result.wasAcknowledged()
        && result.getMatchedCount() == 1
        && result.getModifiedCount() == 1;
  }

  @Override
  protected void doCleanUpCommitCas(
      NonTransactionalOperationContext ctx,
      Hash globalId,
      Set<Hash> branchCommits,
      Set<Hash> newKeyLists,
      Hash refLogId) {
    client.getGlobalLog().deleteOne(Filters.eq(toId(globalId)));

    delete(client.getCommitLog(), branchCommits);
    delete(client.getKeyLists(), newKeyLists);
    client.getRefLog().deleteOne(Filters.eq(toId(refLogId)));
  }

  @Override
  protected void doCleanUpGlobalLog(
      NonTransactionalOperationContext ctx, Collection<Hash> globalIds) {
    delete(client.getGlobalLog(), globalIds);
  }

  @Override
  protected GlobalStatePointer doFetchGlobalPointer(NonTransactionalOperationContext ctx) {
    return loadById(client.getGlobalPointers(), globalPointerKey, GlobalStatePointer::parseFrom);
  }

  @Override
  protected GlobalStateLogEntry doFetchFromGlobalLog(
      NonTransactionalOperationContext ctx, Hash id) {
    return loadById(client.getGlobalLog(), id, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected List<GlobalStateLogEntry> doFetchPageFromGlobalLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(client.getGlobalLog(), hashes, GlobalStateLogEntry::parseFrom);
  }

  @Override
  protected void doWriteRefLog(NonTransactionalOperationContext ctx, AdapterTypes.RefLogEntry entry)
      throws ReferenceConflictException {
    Document id = toId(Hash.of(entry.getRefLogId()));
    insert(client.getRefLog(), toDoc(id, entry.toByteArray()));
  }

  @Override
  protected RefLog doFetchFromRefLog(NonTransactionalOperationContext ctx, Hash refLogId) {
    if (refLogId == null) {
      // set the current head as refLogId
      refLogId = Hash.of(fetchGlobalPointer(ctx).getRefLogId());
    }
    return loadById(client.getRefLog(), refLogId, ProtoSerialization::protoToRefLog);
  }

  @Override
  protected List<RefLog> doFetchPageFromRefLog(
      NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(client.getRefLog(), hashes, ProtoSerialization::protoToRefLog);
  }
}
