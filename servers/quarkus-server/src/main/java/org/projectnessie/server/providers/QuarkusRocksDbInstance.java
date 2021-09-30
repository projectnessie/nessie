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

import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.server.config.VersionStoreConfig;
import org.projectnessie.versioned.persist.rocks.ImmutableRocksDbConfig;
import org.projectnessie.versioned.persist.rocks.RocksDbInstance;

/** CDI bean for {@link QuarkusRocksDbInstance}. */
@Singleton
public class QuarkusRocksDbInstance extends RocksDbInstance {

  @Inject
  public QuarkusRocksDbInstance(VersionStoreConfig.RocksVersionStoreConfig rocksConfig) {
    configure(ImmutableRocksDbConfig.builder().dbPath(rocksConfig.getDbPath()).build());
    initialize();
  }
}
