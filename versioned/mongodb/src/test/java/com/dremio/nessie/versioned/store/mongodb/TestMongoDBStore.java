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

import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.StoreOperationException;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.tests.AbstractTestStore;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.test.publisher.TestPublisher;

/**
 * A test class that contains MongoDB specific tests.
 */
@ExtendWith(LocalMongo.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMongoDBStore extends AbstractTestStore<MongoDBStore> {
  private static final String testDatabaseName = "mydb";
  private String connectionString;

  @BeforeAll
  void init(String connectionString) {
    this.connectionString = "mongodb://localhost";//connectionString;
  }

  @AfterAll
  void close() {
    if (null != store) {
      store.close();
    }
  }

  /**
   * Creates an instance of MongoDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected MongoDBStore createStore() {
    return new MongoDBStore(createConfig());
  }

  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Override
  protected void resetStoreState() {
    store.resetCollections();
  }

  @Test
  void loadExtraInvalidEntity() {
    // Set up the sample data and load it.
    final InternalRef sampleBranch = SampleEntities.createBranch(random);
    final Multimap<ValueType, HasId> objs = ImmutableMultimap.<ValueType, HasId>builder()
        .put(ValueType.REF, sampleBranch)
        .build();
    objs.forEach(this::testPut);
    final LoadStep step = createTestLoadStep(objs);

    // Set up the mocks.
    final TestPublisher<InternalRef> publisher = TestPublisher.<InternalRef>createCold()
        .next(sampleBranch, SampleEntities.createBranch(random))
        .complete();
    final FindPublisher<InternalRef> findPublisher = Mockito.mock(FindPublisher.class, AdditionalAnswers.delegatesTo(publisher));
    final MongoCollection<InternalRef> mockCollection = Mockito.mock(MongoCollection.class);
    Mockito.when(mockCollection.find(ArgumentMatchers.any(Bson.class))).thenReturn(findPublisher);

    // Ensure the mocked collection is returned, which will then return the test publisher.
    final AtomicBoolean hasReturnedCollection = new AtomicBoolean(false);
    final MongoDBStore testStore = new MongoDBStore(createConfig()) {
      @Override
      <T> MongoCollection<T> getCollection(ValueType valueType) {
        try {
          return (MongoCollection<T>) mockCollection;
        } finally {
          hasReturnedCollection.set(true);
        }
      }
    };

    Assertions.assertThrows(StoreOperationException.class, () -> testStore.load(step));
  }

  @Test
  void loadPagination() {
    final ImmutableMultimap.Builder<ValueType, HasId> builder = ImmutableMultimap.builder();
    for (int i = 0; i < (10 + MongoDBStore.LOAD_SIZE); ++i) {
      // Only create a single type as this is meant to test the pagination within Mongo, not the variety. Variety is
      // taken care of by a test in AbstractTestStore.
      builder.put(ValueType.REF, SampleEntities.createTag(random));
    }

    final Multimap<ValueType, HasId> objs = builder.build();
    objs.forEach(this::testPut);

    testLoad(objs);
  }

  private MongoStoreConfig createConfig() {
    return new MongoStoreConfig() {
      @Override
      public String getConnectionString() {
        return connectionString;
      }

      @Override
      public String getDatabaseName() {
        return testDatabaseName;
      }
    };
  }
}
