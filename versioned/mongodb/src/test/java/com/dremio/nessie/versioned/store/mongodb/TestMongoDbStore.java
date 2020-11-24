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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.Iterables;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class TestMongoDbStore {
  private static MongodExecutable mongoExec;
  private static ConnectionString connectionString;
  private static final String testDatabaseName = "mydb";
  private MongoDbStore mongoDbStore;

  /**
   * Set up the embedded flapdoodle MongoDB server for unit tests.
   * @throws IOException if there's an issue grabbing the port or determining IP version.
   */
  @BeforeAll
  public static void setupServer() throws IOException {
    final int port = Network.getFreeServerPort();
    final MongodConfig config = MongodConfig.builder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(port, Network.localhostIsIPv6()))
        .build();

    mongoExec = MongodStarter.getDefaultInstance().prepare(config);
    mongoExec.start();
    connectionString = new ConnectionString("mongodb://localhost:" + port);
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

  /**
   * Set up the objects necessary for the tests.
   */
  @BeforeEach
  public void setUp() {
    mongoDbStore = new MongoDbStore(connectionString, testDatabaseName);
    mongoDbStore.start();
  }

  /**
   * Close the store and clean up written data.
   */
  @AfterEach
  public void teardown() {
    // Clean up the collections.
    // TODO: Is there a better filter to use? Just need one to be true everywhere.
    mongoDbStore.getCollections().forEach((k, v) -> v.deleteMany(Filters.ne("_id", "s")));
    mongoDbStore.close();
  }

  @Test
  public void testDatabaseName() {
    final MongoDatabase mongoDatabase = mongoDbStore.getDatabase();
    assertEquals(testDatabaseName, mongoDatabase.getName());
  }

  @Test
  public void putL1() {
    final L1 sample = SampleEntities.createL1();
    mongoDbStore.put(ValueType.L1, sample, Optional.empty());
    final L1 read = (L1)Iterables.get(mongoDbStore.collections.get(ValueType.L1).find(), 0);
    assertEquals(L1.SCHEMA.itemToMap(sample, true), L1.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putL2() {
    final L2 sample = SampleEntities.createL2();
    mongoDbStore.put(ValueType.L2, sample, Optional.empty());
    final L2 read = (L2)Iterables.get(mongoDbStore.collections.get(ValueType.L2).find(), 0);
    assertEquals(L2.SCHEMA.itemToMap(sample, true), L2.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putL3() {
    final L3 sample = SampleEntities.createL3();
    mongoDbStore.put(ValueType.L3, sample, Optional.empty());
    final L3 read = (L3)Iterables.get(mongoDbStore.collections.get(ValueType.L3).find(), 0);
    assertEquals(L3.SCHEMA.itemToMap(sample, true), L3.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putFragment() {
    final Fragment sample = SampleEntities.createFragment();
    mongoDbStore.put(ValueType.KEY_FRAGMENT, sample, Optional.empty());
    final Fragment read = (Fragment)Iterables.get(mongoDbStore.collections.get(ValueType.KEY_FRAGMENT).find(), 0);
    assertEquals(Fragment.SCHEMA.itemToMap(sample, true), Fragment.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putBranch() {
    final InternalRef sample = SampleEntities.createBranch();
    mongoDbStore.put(ValueType.REF, sample, Optional.empty());
    final InternalRef read = (InternalRef)Iterables.get(mongoDbStore.collections.get(ValueType.REF).find(), 0);
    assertEquals(InternalRef.SCHEMA.itemToMap(sample, true), InternalRef.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putTag() {
    final InternalRef sample = SampleEntities.createTag();
    mongoDbStore.put(ValueType.REF, sample, Optional.empty());
    final InternalRef read = (InternalRef)Iterables.get(mongoDbStore.collections.get(ValueType.REF).find(), 0);
    assertEquals(InternalRef.SCHEMA.itemToMap(sample, true), InternalRef.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putCommitMetadata() {
    final InternalCommitMetadata sample = SampleEntities.createCommitMetadata();
    mongoDbStore.put(ValueType.COMMIT_METADATA, sample, Optional.empty());
    final InternalCommitMetadata read =
        (InternalCommitMetadata)Iterables.get(mongoDbStore.collections.get(ValueType.COMMIT_METADATA).find(), 0);
    assertEquals(
        InternalCommitMetadata.SCHEMA.itemToMap(sample, true),
        InternalCommitMetadata.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void putValue() {
    final InternalValue sample = SampleEntities.createValue();
    mongoDbStore.put(ValueType.VALUE, sample, Optional.empty());
    final InternalValue read = (InternalValue)Iterables.get(mongoDbStore.collections.get(ValueType.VALUE).find(), 0);
    assertEquals(InternalValue.SCHEMA.itemToMap(sample, true), InternalValue.SCHEMA.itemToMap(read, true));
  }

  @Test
  public void loadSingleL1Value() {
    final L1 sampleL1 = SampleEntities.createL1();
    mongoDbStore.put(ValueType.L1, sampleL1, Optional.empty());
    final L1 readL1 = mongoDbStore.loadSingle(ValueType.L1, sampleL1.getId());
    assertEquals(L1.SCHEMA.itemToMap(sampleL1, true), L1.SCHEMA.itemToMap(readL1, true));
  }

  @Test
  public void loadSingleL2Value() {
    final L2 sampleL2 = SampleEntities.createL2();
    mongoDbStore.put(ValueType.L2, sampleL2, Optional.empty());
    final L2 readL2 = mongoDbStore.loadSingle(ValueType.L2, sampleL2.getId());
    assertEquals(L2.SCHEMA.itemToMap(sampleL2, true), L2.SCHEMA.itemToMap(readL2, true));
  }

  @Test
  public void loadSingleL3Value() {
    final L3 sampleL3 = SampleEntities.createL3();
    mongoDbStore.put(ValueType.L3, sampleL3, Optional.empty());
    final L3 readL3 = mongoDbStore.loadSingle(ValueType.L3, sampleL3.getId());
    assertEquals(L3.SCHEMA.itemToMap(sampleL3, true), L3.SCHEMA.itemToMap(readL3, true));
  }
}
