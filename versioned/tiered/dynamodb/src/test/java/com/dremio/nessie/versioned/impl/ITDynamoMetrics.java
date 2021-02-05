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
package com.dremio.nessie.versioned.impl;

import java.util.Collection;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.dremio.nessie.versioned.LocalDynamoDB;
import com.dremio.nessie.versioned.store.ValueType;
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


@ExtendWith(LocalDynamoDB.class)
public class ITDynamoMetrics {
  private static SimpleMeterRegistry registry;
  private Random random;
  private DynamoStore store;

  @BeforeAll
  static void addRegistry() {
    registry = new SimpleMeterRegistry();
    Metrics.globalRegistry.clear();
    Metrics.addRegistry(new SimpleMeterRegistry());
  }

  @AfterAll
  static void removeRegistry() {
    Metrics.removeRegistry(registry);
    Metrics.globalRegistry.clear();
  }

  @BeforeEach
  void buildStore() {
    store = new DynamoStoreFixture().createStoreImpl();
    store.start();
    random = new Random(0L);
  }

  @AfterEach
  void stopStore() {
    store.close();
    store = null;
  }

  @Test
  void testMetrics() {
    store.putIfAbsent(new EntitySaveOp<>(ValueType.REF, SampleEntities.createBranch(random)));

    //make sure standard Dynamo metrics are visible. Expect status codes for each of the 3 dynamo calls made (describe, create, put)
    Assertions.assertTrue(1 <= Metrics.globalRegistry.get("DynamoDB.HttpStatusCode.summary").meters().size());

    //make sure extra Dynamo metrics are visible. Expect capacity for put only
    Collection<Meter> meters = Metrics.globalRegistry.get("DynamoDB.ConsumedCapacity.summary").meters();
    Assertions.assertTrue(1 <= meters.size());
    DistributionSummary putCapacity = (DistributionSummary) meters.stream().findFirst().get();
    Assertions.assertTrue(1 <= putCapacity.count());
    Assertions.assertTrue(1 <= putCapacity.totalAmount());
  }
}
