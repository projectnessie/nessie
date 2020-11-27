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
import java.util.List;
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
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;
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
    put(SampleEntities.createL1(), ValueType.L1, L1.SCHEMA);
  }

  @Test
  public void putL2() {
    put(SampleEntities.createL2(), ValueType.L2, L2.SCHEMA);
  }

  @Test
  public void putL3() {
    put(SampleEntities.createL3(), ValueType.L3, L3.SCHEMA);
  }

  @Test
  public void putFragment() {
    put(SampleEntities.createFragment(), ValueType.KEY_FRAGMENT, Fragment.SCHEMA);
  }

  @Test
  public void putBranch() {
    put(SampleEntities.createBranch(), ValueType.REF, InternalRef.SCHEMA);
  }

  @Test
  public void putTag() {
    put(SampleEntities.createTag(), ValueType.REF, InternalRef.SCHEMA);
  }

  @Test
  public void putCommitMetadata() {
    put(SampleEntities.createCommitMetadata(), ValueType.COMMIT_METADATA, InternalCommitMetadata.SCHEMA);
  }

  @Test
  public void putValue() {
    put(SampleEntities.createValue(), ValueType.VALUE, InternalValue.SCHEMA);
  }

  @Test
  public void loadSingleL1() {
    load(SampleEntities.createL1(), ValueType.L1, L1.SCHEMA);
  }

  @Test
  public void loadSingleL2() {
    load(SampleEntities.createL2(), ValueType.L2, L2.SCHEMA);
  }

  @Test
  public void loadSingleL3() {
    load(SampleEntities.createL3(), ValueType.L3, L3.SCHEMA);
  }

  @Test
  public void loadFragment() {
    load(SampleEntities.createFragment(), ValueType.KEY_FRAGMENT, Fragment.SCHEMA);
  }

  @Test
  public void loadBranch() {
    load(SampleEntities.createBranch(), ValueType.REF, InternalRef.SCHEMA);
  }

  @Test
  public void loadTag() {
    load(SampleEntities.createTag(), ValueType.REF, InternalRef.SCHEMA);
  }

  @Test
  public void loadCommitMetadata() {
    load(SampleEntities.createCommitMetadata(), ValueType.COMMIT_METADATA, InternalCommitMetadata.SCHEMA);
  }

  @Test
  public void loadValue() {
    load(SampleEntities.createValue(), ValueType.VALUE, InternalValue.SCHEMA);
  }

  @Test
  public void save() {
    final L1 l1 = SampleEntities.createL1();
    final InternalRef branch = SampleEntities.createBranch();
    final InternalRef tag = SampleEntities.createTag();
    final List<SaveOp<?>> saveOps = ImmutableList.of(
        new SaveOp<>(ValueType.L1, l1),
        new SaveOp<>(ValueType.REF, branch),
        new SaveOp<>(ValueType.REF, tag)
    );
    mongoDbStore.save(saveOps);

    assertEquals(
        L1.SCHEMA.itemToMap(l1, true),
        L1.SCHEMA.itemToMap(mongoDbStore.loadSingle(ValueType.L1, l1.getId()), true));
    assertEquals(
        InternalRef.SCHEMA.itemToMap(branch, true),
        InternalRef.SCHEMA.itemToMap(mongoDbStore.loadSingle(ValueType.REF, branch.getId()), true));
    assertEquals(
        InternalRef.SCHEMA.itemToMap(tag, true),
        InternalRef.SCHEMA.itemToMap(mongoDbStore.loadSingle(ValueType.REF, tag.getId()), true));
  }

  @SuppressWarnings("unchecked")
  private <T> void put(T sample, ValueType type, SimpleSchema<T> schema) {
    mongoDbStore.put(type, sample, Optional.empty());
    final T read = (T)Iterables.get(mongoDbStore.getCollections().get(type).find(), 0);
    assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
  }

  private <T extends HasId> void load(T sample, ValueType type, SimpleSchema<T> schema) {
    put(sample, type, schema);
    final T read = mongoDbStore.loadSingle(type, sample.getId());
    assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
  }
}
