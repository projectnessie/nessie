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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
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
    final MongoCollection<? extends HasId> collection;
    final List<Id> ids;

    CollectionLoadIds(MongoCollection<? extends HasId> collection, List<Id> ops) {
      this.collection = collection;
      this.ids = ops;
    }
  }

  // Mongo has a 16MB limit on documents, which also pertains to the input query. Given that we use IN for loads,
  // restrict the number of IDs to avoid going above that limit, and to take advantage of the async nature of the
  // requests.
  @VisibleForTesting
  static final int LOAD_SIZE = 1_000;

  private static final HasId FOUND_SENTINEL = () -> Id.EMPTY;

  private final MongoStoreConfig config;
  private final MongoClientSettings mongoClientSettings;

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private final Duration timeout;
  private Map<ValueType, MongoCollection<HasId>> collections;

  /**
   * Creates a store ready for connection to a MongoDB instance.
   * @param config the configuration for the store.
   */
  public MongoDBStore(MongoStoreConfig config) {
    this.config = config;
    this.timeout = Duration.ofMillis(config.getTimeoutMs());
    this.collections = new HashMap<>();
    final CodecRegistry codecRegistry = CodecRegistries.fromProviders(
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
  @SuppressWarnings("unchecked")
  @Override
  public void start() {
    mongoClient = MongoClients.create(mongoClientSettings);
    mongoDatabase = mongoClient.getDatabase(config.getDatabaseName());

    // Initialise collections for each ValueType.
    collections = Stream.of(ValueType.values())
        .collect(ImmutableMap.<ValueType, ValueType, MongoCollection<HasId>>toImmutableMap(
            v -> v,
            v -> {
              String collectionName = v.getTableName(config.getTablePrefix());
              return (MongoCollection<HasId>) mongoDatabase.getCollection(collectionName, v.getObjectClass());
            })
        );

    if (config.initializeDatabase()) {
      // make sure we have an empty l1 (ignore result, doesn't matter)
      putIfAbsent(ValueType.L1, L1.EMPTY);
      putIfAbsent(ValueType.L2, L2.EMPTY);
      putIfAbsent(ValueType.L3, L3.EMPTY);
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void load(LoadStep loadstep) throws NotFoundException {
    for (LoadStep step = loadstep; step != null; step = step.getNext().orElse(null)) {
      final Map<Id, LoadOp<?>> idLoadOps = step.getOps().collect(Collectors.toMap(LoadOp::getId, Function.identity()));

      Flux.fromStream(step.getOps())
        .groupBy(op -> this.getCollection(op.getValueType()))
        .flatMap(entry -> entry.map(LoadOp::getId).buffer(LOAD_SIZE).map(l -> new CollectionLoadIds(entry.key(), l)))
        .flatMap(entry -> entry.collection.find(Filters.in(MongoConsumer.ID, entry.ids)))
        .handle((op, sink) -> {
          // Process each of the loaded entries.
          final LoadOp loadOp = idLoadOps.remove(op.getId());
          if (null == loadOp) {
            sink.error(new StoreOperationException(String.format("Retrieved unexpected object with ID: %s", op.getId())));
          } else {
            loadOp.loaded(op);
            sink.next(op);
          }
        })
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
  public <V extends HasId> boolean putIfAbsent(ValueType type, V value) {
    final MongoCollection<HasId> collection = getCollection(type);

    // Use upsert so that a document is created if the filter does not match. The update operator is only $setOnInsert
    // so no action is triggered on a simple update, only on insert.
    final UpdateResult result = Mono.from(collection.updateOne(
        Filters.eq(MongoConsumer.ID, value.getId()),
        MongoSerDe.bsonForValueType(type, value, "$setOnInsert"),
        new UpdateOptions().upsert(true)
    )).block(timeout);
    return result != null && result.getUpsertedId() != null;
  }

  @Override
  public <V extends HasId> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    Preconditions.checkArgument(type.isEntityType(value),
        "Value class %s is not value for ValueType %s.", value.getClass().getName(), type.name());

    // TODO: Handle ConditionExpressions.
    if (conditionUnAliased.isPresent()) {
      throw new UnsupportedOperationException("ConditionExpressions are not supported with MongoDB yet.");
    }

    final MongoCollection<HasId> collection = getCollection(type);

    // Use upsert so that if an item does not exist, it will be insert.
    final UpdateResult result = Mono.from(
        collection.updateOne(
            Filters.eq(MongoConsumer.ID, value.getId()),
            MongoSerDe.bsonForValueType(type, value, "$set"),
            new UpdateOptions().upsert(true)
        )).block(timeout);
    if (result == null || result.getUpsertedId() == null) {
      throw new StoreOperationException(String.format("Update of %s %s did not succeed", type.name(), value.getId()));
    }
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void save(List<SaveOp<?>> ops) {
    final ListMultimap<MongoCollection<HasId>, SaveOp<?>> mm = Multimaps.index(ops, l -> getCollection(l.getType()));

    Flux.fromIterable(Multimaps.asMap(mm).entrySet())
      .flatMap(entry -> {
        final MongoCollection<HasId> collection = entry.getKey();

        // Ordering of the inserts doesn't matter, so set to unordered to give potential performance improvements.
        List<HasId> entities = entry.getValue().stream()
            .map(saveOp -> (HasId) saveOp.getType().buildEntity(c -> {
              // TODO this should really not use the deviation via a materialized entity but
              //  serialize directly into a series of Bson. Couldn't get the 'insertMany' to
              //  work though.
              BaseConsumer cons = c;
              saveOp.serialize(cons);
            }))
            .collect(Collectors.toList());
        return collection.insertMany(entities, new InsertManyOptions().ordered(false));
      })
        .blockLast(timeout);
  }

  @Override
  public <V extends HasId> V loadSingle(ValueType valueType, Id id) {
    return valueType.buildEntity(c -> loadSingle(valueType, id, c));
  }

  @Override
  public <C extends BaseConsumer<C>> void loadSingle(ValueType valueType, Id id, C consumer) {
    final MongoCollection<HasId> collection = readCollection(valueType, () -> consumer, c -> {});

    Object found = Mono.from(
        collection.find(Filters.eq(MongoConsumer.ID, id))
    ).block(timeout);
    if (null == found) {
      throw new NotFoundException(String.format("Unable to load item with ID: %s", id));
    }
  }

  @SuppressWarnings("rawtypes")
  private MongoCollection<HasId> readCollection(
      ValueType valueType, Supplier<BaseConsumer> consumerSupplier, Consumer<BaseConsumer> onRead) {
    return getCollection(valueType).withCodecRegistry(CodecRegistries.fromCodecs(
        CodecProvider.ID_CODEC_INSTANCE,
        new Codec<HasId>() {
          @SuppressWarnings({"unchecked", "rawtypes"})
          @Override
          public HasId decode(BsonReader reader, DecoderContext decoderContext) {
            BaseConsumer consumer = consumerSupplier.get();
            MongoSerDe.produceToConsumer(reader, valueType, consumer);
            onRead.accept(consumer);
            return FOUND_SENTINEL;
          }

          @Override
          public void encode(BsonWriter writer, HasId value, EncoderContext encoderContext) {
            throw new UnsupportedOperationException();
          }

          @SuppressWarnings("unchecked")
          @Override
          public Class<HasId> getEncoderClass() {
            return (Class<HasId>) valueType.getObjectClass();
          }
        }));
  }

  @Override
  public <V extends HasId> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws NotFoundException {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends HasId> Stream<V> getValues(Class<V> valueClass, ValueType type) {
    // TODO: Can this be optimized to not collect the elements before streaming them?
    // TODO: Could this benefit from paging?
    return (Stream<V>) Flux.from(this.getCollection(type).find()).toStream();
  }

  /**
   * Clear the contents of all the Nessie collections. Only for testing purposes.
   */
  @VisibleForTesting
  void resetCollections() {
    Flux.fromIterable(collections.values()).flatMap(collection -> collection.deleteMany(Filters.ne("_id", "s"))).blockLast(timeout);
  }

  @VisibleForTesting
  MongoCollection<HasId> getCollection(ValueType valueType) {
    final MongoCollection<HasId> collection = collections.get(valueType);
    if (null == collection) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", valueType.name()));
    }
    return collection;
  }
}
