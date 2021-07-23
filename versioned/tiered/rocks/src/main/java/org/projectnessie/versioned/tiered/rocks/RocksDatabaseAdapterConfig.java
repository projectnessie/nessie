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
package org.projectnessie.versioned.tiered.rocks;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;

@Value.Immutable(lazyhash = true)
public interface RocksDatabaseAdapterConfig extends DatabaseAdapterConfig {

  /**
   * Provides the {@link org.rocksdb.RocksDB} instance to {@link RocksDatabaseAdapter}, must be used
   * in production scenarios.
   */
  @Nullable
  RocksDbInstance getDbInstance();

  RocksDatabaseAdapterConfig withDbInstance(RocksDbInstance dbInstance);

  /**
   * Database path for Rocks-DB, only used, if {@link #getDbInstance} returns {@code null}, only for
   * non-production scenarios.
   */
  @Nullable
  String getDbPath();

  RocksDatabaseAdapterConfig withDbPath(String dbPath);
}
