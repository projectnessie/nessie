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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * This class implements the Store interface that is used by Nessie as a backing store for versioning of it's
 * Git like behaviour.
 * The MongoDbStore connects to an external MongoDB server.
 */
public class MongoDBStore implements Store {
  /**
   * A Subscriber that stores the publishers results and provides a latch so can block on completion.
   *
   * <p>Note that this class is taken from the MongoDB Tour:
   * https://github.com/mongodb/mongo-java-driver-reactivestreams/blob/master/examples/tour/src/main/tour/SubscriberHelpers.java
   *
   * @param <T> The publishers result type
   */
  private static class ObservableSubscriber<T> implements Subscriber<T> {
    private final List<T> received;
    private final CountDownLatch latch;
    private Throwable error;
    private boolean hasRequested;
    private volatile Subscription subscription;

    ObservableSubscriber() {
      this.received = new ArrayList<>();
      this.latch = new CountDownLatch(1);
      this.hasRequested = false;
    }

    @Override
    public void onSubscribe(final Subscription s) {
      subscription = s;
    }

    @Override
    public void onNext(final T t) {
      received.add(t);
    }

    @Override
    public void onError(final Throwable t) {
      error = t;
      onComplete();
    }

    @Override
    public void onComplete() {
      latch.countDown();
    }

    T first() {
      return received.isEmpty() ? null : received.get(0);
    }

    List<T> getReceived() {
      return received;
    }

    void request() {
      subscription.request(Integer.MAX_VALUE);
      hasRequested = true;
    }

    ObservableSubscriber<T> await(final long timeout) throws Throwable {
      if (!hasRequested) {
        subscription.request(Integer.MAX_VALUE);
      }

      if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
        throw new TimeoutException("Publisher onComplete timed out");
      }
      if (null != error) {
        throw new ExecutionException(error);
      }
      return this;
    }
  }

  /**
   * Bson implementation for updates, to allow proper encoding of Nessie objects using codecs.
   * @param <T> the entity object type.
   */
  private static class UpdateEntityBson<T> implements Bson {
    private final T value;
    private final Class<T> valueClass;

    public <V> UpdateEntityBson(Class<T> valueClass, T value) {
      this.valueClass = valueClass;
      this.value = value;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(Class<TDocument> clazz, CodecRegistry codecRegistry) {
      // Intentionally don't use Updates.setOnInsert as that will result in issues encoding the value entity,
      // due to codec lookups the MongoDB driver will actually encode the fields of the basic object, not the entity.
      final BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
      writer.writeStartDocument();
      writer.writeName("$setOnInsert");
      final Codec<T> codec = codecRegistry.get(valueClass);
      codec.encode(writer, value, EncoderContext.builder().build());
      writer.writeEndDocument();
      return writer.getDocument();
    }
  }

  private static final Map<ValueType, Function<MongoStoreConfig, String>> typeCollections =
      ImmutableMap.<ValueType, Function<MongoStoreConfig, String>>builder()
          .put(ValueType.L1, MongoStoreConfig::getL1TableName)
          .put(ValueType.L2, MongoStoreConfig::getL2TableName)
          .put(ValueType.L3, MongoStoreConfig::getL3TableName)
          .put(ValueType.REF, MongoStoreConfig::getRefTableName)
          .put(ValueType.VALUE, MongoStoreConfig::getValueTableName)
          .put(ValueType.COMMIT_METADATA, MongoStoreConfig::getMetadataTableName)
          .put(ValueType.KEY_FRAGMENT, MongoStoreConfig::getKeyListTableName)
          .build();

  private final MongoStoreConfig config;
  private final MongoClientSettings mongoClientSettings;

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private final long timeoutMs;
  private final Map<ValueType, MongoCollection<? extends HasId>> collections;

  /**
   * Creates a store ready for connection to a MongoDB instance.
   * @param config the configuration for the store.
   */
  public MongoDBStore(MongoStoreConfig config) {
    this.config = config;
    this.timeoutMs = config.getTimeoutMs();
    this.collections = new HashMap<>();
    final CodecRegistry codecRegistry = CodecRegistries.fromProviders(
        new CodecProvider(),
        PojoCodecProvider.builder().automatic(true).build(),
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
    typeCollections.forEach((k, v) ->
        collections.put(k, (MongoCollection<? extends HasId>)mongoDatabase.getCollection(v.apply(config), k.getObjectClass())));
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
    throw new UnsupportedOperationException();
  }

  @Override
  public <V> boolean putIfAbsent(ValueType type, V value) {
    final MongoCollection<V> collection = getCollection(type);

    // Use upsert so that a document is created if the filter does not match. The update operator is only $setOnInsert
    // so no action is triggered on a simple update, only on insert.
    final UpdateResult result = await(collection.updateOne(
        Filters.eq(Store.KEY_NAME, ((HasId)value).getId()),
        new UpdateEntityBson<>((Class<V>)type.getObjectClass(), value),
        new UpdateOptions().upsert(true))).first();
    return result.getUpsertedId() != null;
  }

  @Override
  public <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    Preconditions.checkArgument(type.getObjectClass().isAssignableFrom(value.getClass()),
        "ValueType %s doesn't extend expected type %s.", value.getClass().getName(), type.getObjectClass().getName());

    // TODO: Handle ConditionExpressions.
    if (conditionUnAliased.isPresent()) {
      throw new UnsupportedOperationException("ConditionExpressions are not supported with MongoDB yet.");
    }

    final MongoCollection<V> collection = getCollection(type);

    // Use upsert so that if an item does not exist, it will be insert.
    await(collection.replaceOne(Filters.eq(Store.KEY_NAME, ((HasId)value).getId()), value, new ReplaceOptions().upsert(true)));
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
    final ListMultimap<MongoCollection<?>, SaveOp<?>> mm = Multimaps.index(ops, l -> collections.get(l.getType()));
    final ListMultimap<MongoCollection<?>, Object> collectionWrites = Multimaps.transformValues(mm, SaveOp::getValue);

    final List<ObservableSubscriber<InsertManyResult>> subscribers = new ArrayList<>();
    for (MongoCollection collection : collectionWrites.keySet()) {
      final ObservableSubscriber<InsertManyResult> subscriber = new ObservableSubscriber<>();
      subscribers.add(subscriber);

      // Ordering of the inserts doesn't matter, so set to unordered to give potential performance improvements.
      collection.insertMany(collectionWrites.get(collection), new InsertManyOptions().ordered(false)).subscribe(subscriber);
      subscriber.request();
    }

    // Wait for each of the writes to have completed.
    subscribers.forEach(s -> {
      try {
        s.await(this.timeoutMs);
      } catch (Throwable throwable) {
        Throwables.throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
      }
    });
  }

  @Override
  public <V> V loadSingle(ValueType valueType, Id id) {
    final MongoCollection<V> collection = getCollection(valueType);

    final V value = await(collection.find(Filters.eq(Store.KEY_NAME, id))).first();
    if (null == value) {
      throw new RuntimeException("Unable to load item with ID: " + id);
    }
    return value;
  }

  @Override
  public <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws NotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<InternalRef> getRefs() {
    // TODO: Can this be optimized to not collect the elements before streaming them?
    return await(((MongoCollection<InternalRef>)getCollection(ValueType.REF)).find()).getReceived().stream();
  }

  /**
   * Clear the contents of all the Nessie collections. Only for testing purposes.
   */
  @VisibleForTesting
  void resetCollections() {
    collections.forEach((k, v) -> await(v.deleteMany(Filters.ne("_id", "s"))));
  }

  /**
   * Given an async publisher, wait for results to arrive and return the subscriber with the result..
   * @param publisher the publisher to wait for results with.
   * @param <T> the type of the result from the publisher.
   * @return the subscriber containing the results of the publisher.
   */
  private <T> ObservableSubscriber<T> await(Publisher<T> publisher) {
    try {
      final ObservableSubscriber<T> subscriber = new ObservableSubscriber<>();
      publisher.subscribe(subscriber);
      return subscriber.await(this.timeoutMs);
    } catch (Throwable throwable) {
      Throwables.throwIfUnchecked(throwable);
      throw new RuntimeException(throwable);
    }
  }

  private MongoCollection getCollection(ValueType valueType) {
    final MongoCollection collection = collections.get(valueType);
    if (null == collection) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", valueType.name()));
    }
    return collection;
  }
}
