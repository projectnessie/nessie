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
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ListMultimap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * This class implements the Store interface that is used by Nessie as a backing store for versioning of it's
 * Git like behaviour.
 * The MongoDbStoreManager connects to an external MongoDB server.
 */
public class MongoDbStoreManager implements Store {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbStoreManager.class);

  //TODO make the server URL configurable since this will currently connected to localhost:27018.
  // The client that connects to the MongoDB server.
  protected MongoClient mongoClient;
  // The database hosted by the MongoDB server.
  private MongoDatabase mongoDatabase;

  public MongoDbStoreManager() {
  }

  /**
   * Creates a connection to an existing database or creates the database if it does not exist.
   * Since MongoDB creates databases and tables if they do not exist, there is no need to validate the presence of
   * either before they are used.
   */
  @Override
  public void start() {
    mongoClient = MongoClients.create();
    //TODO get this from a builder object, so it can be passed in on construction.
    mongoDatabase = mongoClient.getDatabase("mydb");
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
    } else {
      LOGGER.error("Attempt to close connection to MongoDB when no connection exists");
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
