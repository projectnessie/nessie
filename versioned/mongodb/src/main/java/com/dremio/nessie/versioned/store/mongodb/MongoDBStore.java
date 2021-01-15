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
package com.dremio.nessie.versioned.store.mongodb;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.impl.EntityTypeBridge;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.ValuesMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class implements the Store interface that is used by Nessie as a backing store for versioning of it's
 * Git like behaviour.
 * The MongoDbStore connects to an external MongoDB server.
 */
public class MongoDBStore implements Store {

  /**
   * Pair of a collection to the set of IDs to be loaded.
   */
  private static class CollectionLoadIds {
    final MongoCollection<Document> collection;
    final List<Id> ids;

    CollectionLoadIds(MongoCollection<Document> collection, List<Id> ops) {
      this.collection = collection;
      this.ids = ops;
    }
  }

  // Mongo has a 16MB limit on documents, which also pertains to the input query. Given that we use IN for loads,
  // restrict the number of IDs to avoid going above that limit, and to take advantage of the async nature of the
  // requests.
  @VisibleForTesting
  static final int LOAD_SIZE = 1_000;

  private final MongoStoreConfig config;
  private final MongoClientSettings mongoClientSettings;

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private final CodecRegistry codecRegistry;
  private final Duration timeout;
  private Map<ValueType, MongoCollection<Document>> collections;

  /**
   * Creates a store ready for connection to a MongoDB instance.
   * @param config the configuration for the store.
   */
  public MongoDBStore(MongoStoreConfig config) {
    this.config = config;
    this.timeout = Duration.ofMillis(config.getTimeoutMs());
    this.collections = new HashMap<>();
    this.codecRegistry = CodecRegistries.fromProviders(
        new CodecProvider(),
        MongoClientSettings.getDefaultCodecRegistry());
    this.mongoClientSettings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(config.getConnectionString()))
      .codecRegistry(codecRegistry)
      .writeConcern(WriteConcern.MAJORITY)
      .build();
  }

  /**
   * Gets a handle to an existing database or get a handle to a MongoDatabase instance if it does not exist. The new
   * database will be lazily created.
   * Since MongoDB creates databases and collections if they do not exist, there is no need to validate the presence of
   * either before they are used. This creates or retrieves collections that map 1:1 to the enumerates in
   * {@link com.dremio.nessie.versioned.store.ValueType}
   */
  @Override
  public void start() {
    mongoClient = MongoClients.create(mongoClientSettings);
    mongoDatabase = mongoClient.getDatabase(config.getDatabaseName());

    // Initialise collections for each ValueType.
    collections = Stream.of(ValueType.values())
        .collect(ImmutableMap.<ValueType, ValueType, MongoCollection<Document>>toImmutableMap(
            v -> v,
            v -> {
              String collectionName = v.getTableName(config.getTablePrefix());
              return mongoDatabase.getCollection(collectionName);
            })
        );

    if (config.initializeDatabase()) {
      // make sure we have an empty l1 (ignore result, doesn't matter)
      EntityTypeBridge.storeMinimumEntities(this::putIfAbsent);
    }
  }

  /**
   * Closes the connection this manager creates to a database. If the connection is already closed this method has
   * no effect.
   */
  @Override
  public void close() {
    if (null != mongoClient) {
      mongoClient.close();
    }
  }

  @Override
  public void load(LoadStep loadstep) throws NotFoundException {
    for (LoadStep step = loadstep; step != null; step = step.getNext().orElse(null)) {
      final Map<Id, LoadOp<?>> idLoadOps = step.getOps().collect(Collectors.toMap(LoadOp::getId, Function.identity()));

      Flux.fromStream(step.getOps())
        .groupBy(LoadOp::getValueType)
        .flatMap(entry -> {
          ValueType type = entry.key();

          MongoCollection<Document> collection = readCollection(
              type,
              id -> idLoadOps.get(id).getReceiver(),
              id -> idLoadOps.remove(id).done());

          return entry.map(LoadOp::getId)
              .buffer(LOAD_SIZE)
              .map(l -> new CollectionLoadIds(collection, l));
        })
          .flatMap(entry -> entry.collection.find(Filters.in(MongoConsumer.ID, entry.ids)))
          .blockLast(timeout);

      // Check if there were any missed ops.
      final Collection<String> missedIds = idLoadOps.values().stream()
          .map(e -> e.getId().toString())
          .collect(Collectors.toList());
      if (!missedIds.isEmpty()) {
        throw new NotFoundException(String.format("Requested object IDs missing: %s", String.join(", ", missedIds)));
      }
    }
  }

  @Override
  public <C extends BaseConsumer<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
    final MongoCollection<Document> collection = getCollection(saveOp.getType());

    // Use upsert so that a document is created if the filter does not match. The update operator is only $setOnInsert
    // so no action is triggered on a simple update, only on insert.
    final UpdateResult result = Mono.from(collection.updateOne(
        Filters.eq(MongoConsumer.ID, saveOp.getId()),
        MongoSerDe.bsonForValueType(saveOp, "$setOnInsert"),
        new UpdateOptions().upsert(true)
    )).block(timeout);
    return result != null && result.getUpsertedId() != null;
  }

  @Override
  public <C extends BaseConsumer<C>> void put(SaveOp<C> saveOp, Optional<ConditionExpression> conditionUnAliased) {
    // TODO: Handle ConditionExpressions.
    if (conditionUnAliased.isPresent()) {
      throw new UnsupportedOperationException("ConditionExpressions are not supported with MongoDB yet.");
    }

    final MongoCollection<Document> collection = getCollection(saveOp.getType());

    // Use upsert so that if an item does not exist, it will be insert.
    final UpdateResult result = Mono.from(
        collection.updateOne(
            Filters.eq(MongoConsumer.ID, saveOp.getId()),
            MongoSerDe.bsonForValueType(saveOp, "$set"),
            new UpdateOptions().upsert(true)
        )).block(timeout);
    if (result == null || result.getUpsertedId() == null) {
      throw new StoreOperationException(String.format("Update of %s %s did not succeed", saveOp.getType().name(), saveOp.getId()));
    }
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void save(List<SaveOp<?>> ops) {

    Map<ValueType, List<SaveOp>> perType = ops.stream()
        .collect(Collectors.groupingBy(SaveOp::getType));

    Flux.fromIterable(perType.entrySet())
      .flatMap(entry -> {
        ValueType type = entry.getKey();

        Codec<SaveOp> codec = new Codec<SaveOp>() {
          @Override
          public SaveOp decode(BsonReader bsonReader, DecoderContext decoderContext) {
            throw new UnsupportedOperationException();
          }

          @SuppressWarnings("unchecked")
          @Override
          public void encode(BsonWriter bsonWriter, SaveOp o, EncoderContext encoderContext) {
            MongoSerDe.serializeEntity(bsonWriter, o);
          }

          @Override
          public Class<SaveOp> getEncoderClass() {
            return SaveOp.class;
          }
        };

        MongoCollection collection = getCollection(type, codec);

        return collection.insertMany(entry.getValue(), new InsertManyOptions().ordered(false));
      })
        .blockLast(timeout);
  }

  @Override
  public <C extends BaseConsumer<C>> void loadSingle(ValueType valueType, Id id, C consumer) {
    final MongoCollection<Document> collection = readCollection(valueType, x -> consumer, x -> {});

    Object found = Mono.from(
        collection.find(Filters.eq(MongoConsumer.ID, id))
    ).block(timeout);
    if (null == found) {
      throw new NotFoundException(String.format("Unable to load item with ID: %s", id));
    }
  }

  @SuppressWarnings("rawtypes")
  private MongoCollection<Document> readCollection(ValueType type, Function<Id, BaseConsumer> onIdParsed, Consumer<Id> parsed) {
    Codec<Document> codec = new Codec<Document>() {
      @Override
      public Document decode(BsonReader bsonReader, DecoderContext decoderContext) {
        MongoSerDe.produceToConsumer(bsonReader, type, onIdParsed, parsed);
        return new Document();
      }

      @Override
      public void encode(BsonWriter bsonWriter, Document o, EncoderContext encoderContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Class<Document> getEncoderClass() {
        return Document.class;
      }
    };

    return this.getCollection(type, codec);
  }

  @Override
  public <C extends BaseConsumer<C>> boolean update(ValueType type, Id id, UpdateExpression update,
      Optional<ConditionExpression> condition, Optional<BaseConsumer<C>> consumer) throws NotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C extends BaseConsumer<C>, V> Stream<V> getValues(Class<V> valueClass, ValueType type, ValuesMapper<C, V> valuesMapper) {
    // TODO: Can this be optimized to not collect the elements before streaming them?
    // TODO: Could this benefit from paging?
    return Flux.from(this.getCollection(type).find()).toStream()
        .map(d -> {
          C producer = valuesMapper.producerForItem();
          BsonDocument bsonDoc = d.toBsonDocument(Document.class, codecRegistry);
          MongoSerDe.produceToConsumer(bsonDoc, type, producer);
          return valuesMapper.itemProduced(producer);
        });
  }

  /**
   * Clear the contents of all the Nessie collections. Only for testing purposes.
   */
  @VisibleForTesting
  void resetCollections() {
    Flux.fromIterable(collections.values()).flatMap(collection -> collection.deleteMany(Filters.ne("_id", "s"))).blockLast(timeout);
  }

  @VisibleForTesting
  MongoCollection<Document> getCollection(ValueType valueType, Codec<?> codec) {
    return getCollection(valueType).withCodecRegistry(
        new CodecRegistry() {
          @Override
          public <T> Codec<T> get(Class<T> clazz, CodecRegistry codecRegistry) {
            return get(clazz);
          }

          @SuppressWarnings("unchecked")
          @Override
          public <T> Codec<T> get(Class<T> clazz) {
            if (clazz == Id.class) {
              return (Codec<T>) CodecProvider.ID_CODEC_INSTANCE;
            }
            return (Codec<T>) codec;
          }
        });
  }

  MongoCollection<Document> getCollection(ValueType valueType) {
    final MongoCollection<Document> collection = collections.get(valueType);
    if (null == collection) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", valueType.name()));
    }
    return collection;
  }
}
