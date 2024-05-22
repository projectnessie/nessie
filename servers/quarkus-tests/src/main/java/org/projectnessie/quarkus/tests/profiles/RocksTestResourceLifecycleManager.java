/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.quarkus.tests.profiles;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.projectnessie.versioned.storage.rocksdbtests.RocksDBBackendTestFactory;

public class RocksTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  private RocksDBBackendTestFactory rocksdb;

  @Override
  public Map<String, String> start() {
    rocksdb = new RocksDBBackendTestFactory();

    try {
      rocksdb.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return rocksdb.getQuarkusConfig();
  }

  @Override
  public void stop() {
    if (rocksdb != null) {
      try {
        rocksdb.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
