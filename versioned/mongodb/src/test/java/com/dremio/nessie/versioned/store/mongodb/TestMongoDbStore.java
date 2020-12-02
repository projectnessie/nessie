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

import java.io.IOException;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.tests.AbstractTestStore;
import com.google.common.collect.ImmutableSet;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

/**
 * A test class that contains MongoDB specific tests.
 */
class TestMongoDbStore extends AbstractTestStore<MongoDbStore> {
  private MongoStoreConfig mongoStoreConfig;
  private static final String testDatabaseName = "mydb";
  private static final String adminDatabaseName = "admin";
  private static final LocalMongo localMongo = LocalMongo.getInstance();

  /**
   * Set up the embedded flapdoodle MongoDB server for unit tests.
   * @throws IOException if there's an issue grabbing the port or determining IP version.
   */
  @BeforeAll
  public static void setupServer() throws IOException {
    localMongo.setupServer();
  }

  /**
   * Shut down the embedded flapdoodle MongoDB server.
   */
  @AfterAll
  public static void teardownServer() {
    localMongo.teardownServer();
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the instance
   */
  @Override
  protected MongoDbStore createStore() {
    mongoStoreConfig = new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return localMongo.getConnectionString();
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    };

    return new MongoDbStore(mongoStoreConfig);
  }

  @Override
  protected void resetStoreState() {
    store.resetCollections();
  }

  @Override
  protected void postStartActions() {
    MongoDatabase adminMongoDatabase = createBasicMongoDBStore(adminDatabaseName);
    IndexManager.getInstance().createIndexOnCollection(adminMongoDatabase, store.getDatabase(),
        getCollections(store.getDatabase(), mongoStoreConfig));
  }

  /**
   * Creates a MongoDB database with minimum configuration.
   * @param databaseName the database
   * @return a database object
   */
  private MongoDatabase createBasicMongoDBStore(String databaseName) {
    MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(localMongo.getConnectionString()))
        .build();
    MongoClient mongoClient = MongoClients.create(mongoClientSettings);
    return mongoClient.getDatabase(databaseName);
  }

  private static Set<MongoCollection> getCollections(MongoDatabase mongoDatabase, MongoStoreConfig config) {
    return ImmutableSet.of(
      mongoDatabase.getCollection(config.getL1TableName(), L1.class),
      mongoDatabase.getCollection(config.getL2TableName(), L2.class),
      mongoDatabase.getCollection(config.getL3TableName(), L3.class),
      mongoDatabase.getCollection(config.getMetadataTableName(), InternalCommitMetadata.class),
      mongoDatabase.getCollection(config.getKeyListTableName(), Fragment.class),
      mongoDatabase.getCollection(config.getRefTableName(), InternalRef.class),
      mongoDatabase.getCollection(config.getValueTableName(), InternalValue.class));
  }
}
