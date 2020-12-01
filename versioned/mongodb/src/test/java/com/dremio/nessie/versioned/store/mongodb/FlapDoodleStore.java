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
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

/**
 * Creates and configures a flapdoodle MongoDB instance.
 */
public class FlapDoodleStore {
  protected static final String testDatabaseName = "mydb";
  protected static final String adminDatabaseName = "admin";

  private static MongodExecutable mongoExec;
  private static String connectionString;
  private static MongoDbStore mongoDbStore;
  private static MongoStoreConfig mongoStoreConfig;


  /**
   * Private constructor.
   */
  private FlapDoodleStore() {
  }

  /**
   * Set up the embedded flapdoodle MongoDB server for unit tests.
   * @throws IOException if there's an issue grabbing the port or determining IP version.
   */
  public static void setupServer() throws IOException {
    final int port = Network.getFreeServerPort();
    final MongodConfig config = MongodConfig.builder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(port, Network.localhostIsIPv6()))
        .build();

    mongoExec = MongodStarter.getDefaultInstance().prepare(config);
    mongoExec.start();
    connectionString = "mongodb://localhost:" + port;
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the instance
   */
  public static MongoDbStore createStore() {
    mongoStoreConfig = new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return connectionString;
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    };

    mongoDbStore = new MongoDbStore(mongoStoreConfig);

    return mongoDbStore;
  }

  /**
   * Shut down the embedded flapdoodle MongoDB server.
   */
  @AfterAll
  public static void teardownServer() {
    if (null != mongoExec) {
      mongoExec.stop();
    }
  }

  static void postStartActions() {
    MongoDbStore adminMongoDbStore = new MongoDbStore(new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return connectionString;
      }

      @Override
      public String getDatabaseName() {
        return adminDatabaseName;
      }
    });
    adminMongoDbStore.start();

    IndexManager.createIndexOnCollection(mongoDbStore.getDatabase(), adminMongoDbStore.getDatabase(),
        getCollections(mongoDbStore.getDatabase(), mongoStoreConfig));
  }

  private static Set<MongoCollection> getCollections(MongoDatabase mongoDatabase, MongoStoreConfig config) {
    Set<MongoCollection> mongoCollections = new HashSet<>();
    mongoCollections.add(mongoDatabase.getCollection(config.getL1TableName(), L1.class));
    mongoCollections.add(mongoDatabase.getCollection(config.getL2TableName(), L2.class));
    mongoCollections.add(mongoDatabase.getCollection(config.getL3TableName(), L3.class));
    mongoCollections.add(mongoDatabase.getCollection(config.getMetadataTableName(), InternalCommitMetadata.class));
    mongoCollections.add(mongoDatabase.getCollection(config.getKeyListTableName(), Fragment.class));
    mongoCollections.add(mongoDatabase.getCollection(config.getRefTableName(), InternalRef.class));
    mongoCollections.add(mongoDatabase.getCollection(config.getValueTableName(), InternalValue.class));

    return mongoCollections;
  }
}
