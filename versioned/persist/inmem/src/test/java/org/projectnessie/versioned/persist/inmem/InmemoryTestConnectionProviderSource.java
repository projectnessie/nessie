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
package org.projectnessie.versioned.persist.inmem;

import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.TestConnectionProviderSource;

public class InmemoryTestConnectionProviderSource
    implements TestConnectionProviderSource<InmemoryStore> {

  private InmemoryStore store;

  @Override
  public DatabaseAdapterConfig<InmemoryStore> updateConfig(
      DatabaseAdapterConfig<InmemoryStore> config) {
    return config.withConnectionProvider(store);
  }

  @Override
  public void start() {
    store = new InmemoryStore();
  }

  @Override
  public void stop() {
    store = null;
  }
}
