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

import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;

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
    this.connectionString = connectionString;
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
    // Create the mocks necessary to override behaviour.
    final FindPublisher<InternalRef> mockPublisher = Mockito.mock(FindPublisher.class);
    final ArgumentCaptor<Subscriber<InternalRef>> subCaptor = ArgumentCaptor.forClass(Subscriber.class);
    Mockito.doNothing().when(mockPublisher).subscribe(subCaptor.capture());

    final MongoCollection<InternalRef> mockCollection = Mockito.mock(MongoCollection.class);
    Mockito.when(mockCollection.find(ArgumentMatchers.any(Bson.class))).thenReturn(mockPublisher);

    // Ensure our mocked collection is returned.
    final MongoDBStore testStore = new MongoDBStore(createConfig()) {
      @Override
      <T> MongoCollection<T> getCollection(ValueType valueType) {
        return (MongoCollection<T>) mockCollection;
      }
    };

    final InternalRef sampleBranch = SampleEntities.createBranch(random);
    final Multimap<ValueType, HasId> objs = ImmutableMultimap.<ValueType, HasId>builder()
        .put(ValueType.REF, sampleBranch)
        .build();
    objs.forEach(this::putThenLoad);
    final LoadStep step = createTestLoadStep(objs);

    // Since the operations are async, start a thread to wait a bit of time before returning the extra entity.
    new Thread(() -> {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        Assertions.fail();
      }
      subCaptor.getValue().onNext(sampleBranch);
      subCaptor.getValue().onNext(SampleEntities.createBranch(random));
    }).start();
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
    objs.forEach(this::putThenLoad);

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
