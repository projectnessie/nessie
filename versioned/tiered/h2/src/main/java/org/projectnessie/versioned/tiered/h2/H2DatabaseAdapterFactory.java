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
package org.projectnessie.versioned.tiered.h2;

import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.tx.ImmutableDefaultTxDatabaseAdapterConfig;
import org.projectnessie.versioned.tiered.tx.TxDatabaseAdapterConfig;

public class H2DatabaseAdapterFactory implements DatabaseAdapterFactory<TxDatabaseAdapterConfig> {

  @Override
  public String getName() {
    return "H2";
  }

  @Override
  public Builder<TxDatabaseAdapterConfig> newBuilder() {
    return new Builder<TxDatabaseAdapterConfig>() {
      @Override
      protected TxDatabaseAdapterConfig getDefaultConfig() {
        return ImmutableDefaultTxDatabaseAdapterConfig.builder()
            .jdbcUrl("jdbc:h2:mem:nessie")
            .build();
      }

      @Override
      public DatabaseAdapter build() {
        return new H2DatabaseAdapter(getConfig());
      }
    };
  }
}
