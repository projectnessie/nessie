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
package com.dremio.nessie.serverless;

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.services.TableCommitMetaStoreWorker;
import com.dremio.nessie.services.TableCommitMetaStoreWorkerImpl;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.impl.DynamoStore;
import com.dremio.nessie.versioned.impl.DynamoStoreConfig;
import com.dremio.nessie.versioned.impl.DynamoVersionStore;

@Singleton
public class VersionStoreProducer {

  @Produces
  public TableCommitMetaStoreWorker worker() {
    return new TableCommitMetaStoreWorkerImpl();
  }

  /**
   * default config for lambda function.
   */
  @Produces
  public VersionStore<Table, CommitMeta> configuration(TableCommitMetaStoreWorker storeWorker, DynamoStore store) {
    return new DynamoVersionStore<>(storeWorker, store, false);
  }

  @Produces
  public DynamoStore dyanmo() {
    return new DynamoStore(DynamoStoreConfig.builder().build()); //todo
  }
}
