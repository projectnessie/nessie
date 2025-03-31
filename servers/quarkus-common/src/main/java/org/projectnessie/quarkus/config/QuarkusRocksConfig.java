/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.nio.file.Path;
import org.projectnessie.versioned.storage.rocksdb.RocksDBBackendBaseConfig;

/**
 * When setting {@code nessie.version.store.type=ROCKSDB} which enables RocksDB as the version store
 * used by the Nessie server, the following configurations are applicable.
 */
@ConfigMapping(prefix = "nessie.version.store.persist.rocks")
public interface QuarkusRocksConfig extends RocksDBBackendBaseConfig {

  @WithName("database-path")
  @WithDefault("/tmp/nessie-rocksdb-store")
  @Override
  Path databasePath();
}
