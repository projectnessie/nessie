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

import static org.projectnessie.server.config.VersionStoreConfig.VersionStoreType.TIERED_INMEMORY;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import org.projectnessie.server.config.TieredInMemVersionStoreConfig;
import org.projectnessie.versioned.inmem.InMemStore;
import org.projectnessie.versioned.store.Store;

@StoreType(TIERED_INMEMORY)
@Dependent
public class TieredInMemVersionStoreFactory extends TieredVersionStoreFactory {

  @Inject
  public TieredInMemVersionStoreFactory(TieredInMemVersionStoreConfig config) {
    super(config);
  }

  @Override
  protected Store createStore() {
    return new InMemStore();
  }
}
