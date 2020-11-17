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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ListMultimap;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * This class implements the Store interface that is used by Nessie as a backing store for versioning of it's
 * Git like behaviour.
 * The MongoDbStore connects to an external MongoDB server.
 */
public class MongoDbStore implements Store {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbStore.class);

  private final int mongoPort = 27017;
  private final String serverName;
  private final String databaseName;

  // The names of the collections in the database. These relate to
  // {@link com.dremio.nessie.versioned.store.ValueType}
  private final String l1Collection = "l1";
  private final String l2Collection = "l2";
  private final String l3Collection = "l3";
  private final String refCollection = "r";
  private final String valueCollection = "v";
  private final String keyFragmentCollection = "k";
  private final String commitMetadataCollection = "m";

  // The client that connects to the MongoDB server.
  protected MongoClient mongoClient;
  // The database hosted by the MongoDB server.
  private MongoDatabase mongoDatabase;
  protected MongoCollection<InternalValue> valueMongoCollection;
  protected MongoCollection<L1> l1MongoCollection;
  protected MongoCollection<L2> l2MongoCollection;
  protected MongoCollection<L3> l3MongoCollection;

  /**
   * Creates a store ready for connection to a MongoDB instance.
   * @param serverName the server on which the MongoDB instance is hosted. Eg localhost.
   * @param databaseName the name of the database to retrieve
   */
  public MongoDbStore(String serverName, String databaseName) {
    this.serverName = serverName;
    this.databaseName = databaseName;
  }

  /**
   * Creates a connection to an existing database or creates the database if it does not exist.
   * Since MongoDB creates databases and collections if they do not exist, there is no need to validate the presence of
   * either before they are used. This creates or retrieves references collections that map 1:1 to the enumerates in
   * {@link com.dremio.nessie.versioned.store.ValueType}
   */
  @Override
  public void start() {
    mongoClient = MongoClients.create(MongoClientSettings.builder()
      .applyToClusterSettings(builder ->
        builder.hosts(Arrays.asList(new ServerAddress(serverName, mongoPort))))
      .build());
    mongoDatabase = mongoClient.getDatabase(databaseName);

    //Initialise collections for each ValueType.
    l1MongoCollection = mongoDatabase.getCollection(l1Collection, L1.class);
    l2MongoCollection = mongoDatabase.getCollection(l2Collection, L2.class);
    l3MongoCollection = mongoDatabase.getCollection(l3Collection, L3.class);
    MongoCollection<InternalRef> refMongoCollection = mongoDatabase.getCollection(refCollection, InternalRef.class);
    valueMongoCollection = mongoDatabase.getCollection(valueCollection, InternalValue.class);
    MongoCollection<Fragment> keyFragmentMongoCollection = mongoDatabase.getCollection(keyFragmentCollection, Fragment.class);
    MongoCollection<InternalCommitMetadata> commitMetadataMongoCollection =
        mongoDatabase.getCollection(commitMetadataCollection, InternalCommitMetadata.class);
  }

  /**
   * Closes the connection this manager creates to a database. If the connection is already closed this method has
   * no effect.
   */
  @Override
  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
  }

  @Override
  public void load(LoadStep loadstep) throws ReferenceNotFoundException {
  }

  private List<ListMultimap<String, LoadOp<?>>> paginateLoads(LoadStep loadStep, int size) {
    List<ListMultimap<String, LoadOp<?>>> paginated = new ArrayList<>();
    return paginated;
  }

  @Override
  public <V> boolean putIfAbsent(ValueType type, V value) {
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> void put(ValueType type, V value, Optional<ConditionExpression> conditionUnAliased) {
    // TODO ensure that calls to mongoDatabase.createCollection etc are surrounded with try-catch to detect
    //  com.mongodb.MongoSocketOpenException
    LOGGER.info("Connected to ", mongoDatabase.toString());
    LOGGER.info("ValueType: " + type.toString() + " Value: " + ((L2)value).toString());
    if (type.equals(ValueType.L2)) {
      LOGGER.info("About to insert value");
      l2MongoCollection.insertOne((L2)value);
      LOGGER.info("Inserted value");
    }
  }

  @Override
  public boolean delete(ValueType type, Id id, Optional<ConditionExpression> condition) {
    return false;
  }

  @Override
  public void save(List<SaveOp<?>> ops) {
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V loadSingle(ValueType valueType, Id id) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> Optional<V> update(ValueType type, Id id, UpdateExpression update, Optional<ConditionExpression> condition)
      throws ReferenceNotFoundException {
    return Optional.empty();
  }

  private final boolean tableExists(String name) {
    return false;
  }

  @Override
  public Stream<InternalRef> getRefs() {
    return null;
  }
}
