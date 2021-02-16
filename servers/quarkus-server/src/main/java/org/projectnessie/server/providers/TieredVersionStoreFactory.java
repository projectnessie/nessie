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
package org.projectnessie.server.providers;

import java.io.IOException;

import org.projectnessie.server.config.TieredVersionStoreConfig;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.impl.TieredVersionStore;
import org.projectnessie.versioned.store.Store;

public abstract class TieredVersionStoreFactory implements VersionStoreFactory {

  private final TieredVersionStoreConfig tieredVersionStoreConfig;

  protected TieredVersionStoreFactory(TieredVersionStoreConfig tieredVersionStoreConfig) {
    this.tieredVersionStoreConfig = tieredVersionStoreConfig;
  }

  @Override
  public <VALUE, METADATA> VersionStore<VALUE, METADATA> newStore(
      StoreWorker<VALUE, METADATA> worker) throws IOException {
    return new TieredVersionStore<>(worker, createStore(),
        org.projectnessie.versioned.impl.TieredVersionStoreConfig.builder()
            .waitOnCollapse(false)
            .commitRetryCount(tieredVersionStoreConfig.commitRetryCount())
            .p2CommitRetryCount(tieredVersionStoreConfig.p2CommitRetryCount())
            .build());
  }

  protected abstract Store createStore();
}
