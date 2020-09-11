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
package com.dremio.nessie.hms;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.RuntimeStatsCleanerTask;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.hms.HMSProto.CommitMetadata;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.impl.DynamoStore;
import com.dremio.nessie.versioned.impl.DynamoStoreConfig;
import com.dremio.nessie.versioned.impl.DynamoVersionStore;
import com.google.common.base.Throwables;

import software.amazon.awssdk.regions.Region;


class TestHMS {

  private static CompletableFuture<Void> HMS_THREAD;

  private static DynamoStore STORE;
  private static DynamoVersionStore<Item, CommitMetadata> DVS;

  @BeforeAll
  static void beforeAll() throws Exception {
    STORE = new DynamoStore(DynamoStoreConfig.builder()
        .endpoint(new URI("http://localhost:8000"))
        .region(Region.US_WEST_2)
        .build());
    STORE.start();
    DVS = new DynamoVersionStore<Item, CommitMetadata>(new HMSRawStoreWorker(), STORE, true);
    try {
      DVS.toHash(BranchName.of("main"));
    } catch (ReferenceNotFoundException r) {
      DVS.create(BranchName.of("main"), Optional.empty());
    }

    System.setProperty(ConfVars.RAW_STORE_IMPL.getVarname(), TraceRawStore.class.getCanonicalName());
    System.setProperty(ConfVars.TASK_THREADS_ALWAYS.getVarname(), EventCleanerTask.class.getName() + "," + RuntimeStatsCleanerTask.class.getName());
    System.setProperty(ConfVars.WAREHOUSE.getVarname(), "/tmp/hive/warehouse");
    System.setProperty(ConfVars.WAREHOUSE_EXTERNAL.getVarname(), "/tmp/hive/ewarehouse");
    System.setProperty(ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.getVarname(), "false");

    Runnable r = () -> {
      try {

        HiveMetaStore.main(new String[]{});
      } catch (Throwable e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    };
    HMS_THREAD = CompletableFuture.runAsync(r);
  }

  @AfterAll
  static void afterAll() {
    HMS_THREAD.cancel(true);
    HMS_THREAD.join();
    STORE.close();
  }

  @Before
  void before() throws Exception {

  }

  @After
  void after() throws Exception {
    DVS.delete(BranchName.of("main"), Optional.empty());
    STORE.close();
  }

  @Test
  public void foo() throws Throwable {
    Thread.sleep(10000000);
  }

}
