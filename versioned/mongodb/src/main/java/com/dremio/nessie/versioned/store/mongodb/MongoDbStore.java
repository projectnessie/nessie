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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
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

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
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
public class MongoDbStore implements Store {
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
    private final List<Throwable> errors;
    private final CountDownLatch latch;
    private volatile Subscription subscription;
    private volatile boolean isCompleted;

    ObservableSubscriber() {
      this.received = new ArrayList<>();
      this.errors = new ArrayList<>();
      this.latch = new CountDownLatch(1);
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
      errors.add(t);
      onComplete();
    }

    @Override
    public void onComplete() {
      isCompleted = true;
      latch.countDown();
    }

    Subscription getSubscription() {
      return subscription;
    }

    T first() {
      return received.isEmpty() ? null : received.get(0);
    }

    List<T> getReceived() {
      return received;
    }

    Throwable getError() {
      return errors.isEmpty() ? null : errors.get(0);
    }

    boolean isCompleted() {
      return isCompleted;
    }

    List<T> get(final long timeout, final TimeUnit unit) throws Throwable {
      return await(timeout, unit).getReceived();
    }

    ObservableSubscriber<T> await() throws Throwable {
      return await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) throws Throwable {
      subscription.request(Integer.MAX_VALUE);
      if (!latch.await(timeout, unit)) {
        throw new MongoTimeoutException("Publisher onComplete timed out");
      }
      if (!errors.isEmpty()) {
        throw errors.get(0);
      }
      return this;
    }
  }

  /**
   * Bson implementation for updates, to allow proper encoding of Nessie objects using codecs.
   * @param <T> the entity object type.
   */
  private static class UpdateEntityBson<T> implements Bson {
    final T value;

    UpdateEntityBson(T value) {
      this.value = value;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(Class<TDocument> clazz, CodecRegistry codecRegistry) {
      // Intentionally don't use Updates.setOnInsert as that will result in issues encoding the value entity,
      // due to codec lookups the MongoDB driver will actually encode the fields of the basic object, not the entity.
      final BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
      writer.writeStartDocument();
      writer.writeName("$setOnInsert");
      // This intentionally doesn't use generics to avoid casting issues around captures.
      Codec codec = codecRegistry.get(clazz);
      codec.encode(writer, value, EncoderContext.builder().build());
      writer.writeEndDocument();
      return writer.getDocument();
    }
  }

  private static final InsertManyOptions INSERT_UNORDERED = new InsertManyOptions().ordered(false);
  private static final UpdateOptions UPDATE_UPSERT = new UpdateOptions().upsert(true);
  private static final ReplaceOptions REPLACE_UPSERT = new ReplaceOptions().upsert(true);
  private static final int SAVE_SIZE = 100_000;
  private static final int LOAD_SIZE = 1_000;

  private final MongoStoreConfig config;
  private final MongoClientSettings mongoClientSettings;

  private MongoClient mongoClient;
  private MongoDatabase mongoDatabase;
  private final Map<ValueType, MongoCollection<? extends HasId>> collections;

  /**
   * Creates a store ready for connection to a MongoDB instance.
   * @param config the configuration for the store.
   */
  public MongoDbStore(MongoStoreConfig config) {
    this.config = config;
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
    collections.put(ValueType.L1, mongoDatabase.getCollection(config.getL1TableName(), L1.class));
    collections.put(ValueType.L2, mongoDatabase.getCollection(config.getL2TableName(), L2.class));
    collections.put(ValueType.L3, mongoDatabase.getCollection(config.getL3TableName(), L3.class));
    collections.put(ValueType.COMMIT_METADATA,
        mongoDatabase.getCollection(config.getMetadataTableName(), InternalCommitMetadata.class));
    collections.put(ValueType.KEY_FRAGMENT,
        mongoDatabase.getCollection(config.getKeyListTableName(), Fragment.class));
    collections.put(ValueType.REF, mongoDatabase.getCollection(config.getRefTableName(), InternalRef.class));
    collections.put(ValueType.VALUE,
        mongoDatabase.getCollection(config.getValueTableName(), InternalValue.class));
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
  public void load(LoadStep loadstep) throws ReferenceNotFoundException {
    for (LoadStep step = loadstep; step != null; step = step.getNext().orElse(null)) {
      final List<ObservableSubscriber> subscribers = new ArrayList<>();
      final List<LoadOp<?>> loadOps = step.getOps().collect(Collectors.toList());
      final Map<Id, LoadOp<?>> idLoadOps = step.getOps().collect(Collectors.toMap(LoadOp::getId, Function.identity()));
      final ListMultimap<MongoCollection<?>, LoadOp<?>> collectionToOps =
          Multimaps.index(loadOps, l -> collections.get(l.getValueType()));

      for (MongoCollection<?> collection : collectionToOps.keySet()) {
        Lists.partition(collectionToOps.get(collection), LOAD_SIZE).forEach(ops -> {
          final List<Id> idList = ops.stream().map(LoadOp::getId).collect(Collectors.toList());
          final ObservableSubscriber<Object> subscriber = new ObservableSubscriber<>();
          subscribers.add(subscriber);
          collection.find(Filters.in(KEY_NAME, idList)).subscribe(subscriber);
        });
      }

      int opsRemaining = loadOps.size();
      for (ObservableSubscriber subscriber : subscribers) {
        await(subscriber);
        opsRemaining -= subscriber.getReceived().size();
        for (Object o : subscriber.getReceived()) {
          final LoadOp<?> loadOp = idLoadOps.get(((HasId)o).getId());
          if (null == loadOp) {
            throw new ReferenceNotFoundException(String.format("Object missing with ID: %s", ((HasId)o).getId()));
          }
          final ValueType type = loadOp.getValueType();
          loadOp.loaded(type.addType(type.getSchema().itemToMap(o, true)));
        }
      }

      if (0 != opsRemaining) {
        throw new ReferenceNotFoundException(
            String.format("[%d] object(s) missing. \n\nLoad Objects: %s", opsRemaining, loadOps));
      }
    }
  }

  @Override
  public <V> boolean putIfAbsent(ValueType type, V value) {
    final MongoCollection<V> collection = (MongoCollection<V>)collections.get(type);
    if (null == collection) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", type.name()));
    }

    final UpdateResult result = await(collection.updateOne(
        Filters.eq(Store.KEY_NAME, ((HasId)value).getId()),
        new UpdateEntityBson<>(value),
        UPDATE_UPSERT)).first();
    return result.getUpsertedId() != null;
  }

  @Override
  public <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    Preconditions.checkArgument(type.getObjectClass().isAssignableFrom(value.getClass()),
        "ValueType %s doesn't extend expected type %s.", value.getClass().getName(), type.getObjectClass().getName());

    final MongoCollection<V> collection = (MongoCollection<V>)collections.get(type);
    if (null == collection) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", type.name()));
    }

    // TODO: Handle ConditionExpressions.
    await(collection.replaceOne(Filters.eq(Store.KEY_NAME, ((HasId)value).getId()), value, REPLACE_UPSERT));
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
      Lists.partition(collectionWrites.get(collection), SAVE_SIZE).forEach(l -> {
        final ObservableSubscriber<InsertManyResult> subscriber = new ObservableSubscriber<>();
        subscribers.add(subscriber);
        collection.insertMany(l, INSERT_UNORDERED).subscribe(subscriber);
      });
    }

    // Wait for each of the writes to have completed.
    awaitAll(subscribers);
  }

  @Override
  public <V> V loadSingle(ValueType valueType, Id id) {
    final MongoCollection<V> collection = (MongoCollection<V>) getCollection(valueType);

    final V value = await(collection.find(Filters.eq(Store.KEY_NAME, id))).first();
    if (null == value) {
      throw new RuntimeException("Unable to load item with ID: " + id);
    }
    return value;
  }

  @Override
  public <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws ReferenceNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Stream<InternalRef> getRefs() {
    return await(((MongoCollection<InternalRef>)getCollection(ValueType.REF)).find()).getReceived().stream();
  }

  @VisibleForTesting
  MongoDatabase getDatabase() {
    return mongoDatabase;
  }

  /**
   * Clear the contents of all the Nessie collections. Only for testing purposes.
   */
  @VisibleForTesting
  void resetCollections() {
    collections.forEach((k, v) -> await(v.deleteMany(Filters.ne("_id", "s"))));
  }

  private <T> ObservableSubscriber<T> await(Publisher<T> publisher) {
    final ObservableSubscriber<T> subscriber = new ObservableSubscriber<>();
    publisher.subscribe(subscriber);
    await(subscriber);
    return subscriber;
  }

  private <T> ObservableSubscriber<T> await(ObservableSubscriber<T> subscriber) {
    try {
      return subscriber.await();
    } catch (Throwable throwable) {
      Throwables.throwIfUnchecked(throwable);
      throw new RuntimeException(throwable);
    }
  }

  private <T> void awaitAll(List<ObservableSubscriber<T>> subscribers) {
    subscribers.forEach(this::await);
  }

  private MongoCollection<? extends HasId> getCollection(ValueType valueType) {
    final MongoCollection<? extends HasId> collection = collections.get(valueType);
    if (null == collection) {
      throw new UnsupportedOperationException(String.format("Unsupported Entity type: %s", valueType.name()));
    }
    return collection;
  }
}
