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
package org.projectnessie.versioned.persist.nontx;

import org.immutables.value.Value;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

public interface NonTransactionalDatabaseAdapterConfig extends DatabaseAdapterConfig {
  int DEFAULT_PARENTS_PER_GLOBAL_COMMIT = 50;
  int DEFAULT_GLOBAL_LOG_ENTRY_SIZE = 250_000;

  /**
   * The number of parent-global-commit-hashes stored in {@link
   * org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry#getParentsList()}.
   * Defaults to {@value #DEFAULT_PARENTS_PER_GLOBAL_COMMIT}.
   */
  @Value.Default
  default int getParentsPerGlobalCommit() {
    return DEFAULT_PARENTS_PER_GLOBAL_COMMIT;
  }

  /**
   * Maximum size of a database object/row.
   *
   * <p>This parameter is respected when compacting the global log via {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter#repoMaintenance(org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams)}.
   *
   * <p>Not all kinds of databases have hard limits on the maximum size of a database object/row.
   *
   * <p>This value must not be "on the edge" - means: it must leave enough room for a somewhat
   * large-ish list,, database-serialization overhead and similar.
   *
   * <p>Values {@code <=0} are illegal, defaults to {@link #getDefaultMaxKeyListSize()}.
   */
  @Value.Default
  default int getGlobalLogEntrySize() {
    return getDefaultGlobalLogEntrySize();
  }

  /**
   * Database adapter implementations that actually do have a hard technical or highly recommended
   * limit on a maximum db-object / db-row size limitation should override this method and return a
   * "good" value.
   *
   * <p>As for {@link #getGlobalLogEntrySize()}, this value must not be "on the edge" - means: it
   * must leave enough room for a somewhat large-ish list, database-serialization overhead * and
   * similar.
   *
   * <p>Defaults to {@value #DEFAULT_GLOBAL_LOG_ENTRY_SIZE}.
   */
  default int getDefaultGlobalLogEntrySize() {
    return DEFAULT_GLOBAL_LOG_ENTRY_SIZE;
  }
}
