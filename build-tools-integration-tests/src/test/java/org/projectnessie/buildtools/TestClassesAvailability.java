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

package org.projectnessie.buildtools;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestClassesAvailability {
  @ParameterizedTest
  @ValueSource(strings = {
    // from nessie-model
    "org.projectnessie.model.ImmutableIcebergTable",
    // from nessie-versioned-storage-common
    "org.projectnessie.versioned.storage.common.logic.Logics",
    // from nessie-versioned-storage-common-serialize
    "org.projectnessie.versioned.storage.serialize.ProtoSerialization",
    // from nessie-versioned-storage-store
    "org.projectnessie.versioned.storage.versionstore.VersionStoreImpl",
    // from nessie-versioned-storage-inmemory-tests
    "org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory",
    // from nessie-versioned-storage-rocksdb-tests
    "org.projectnessie.versioned.storage.rocksdbtests.RocksDBBackendTestFactory",
    // from nessie-versioned-storage-mongodb2
    "org.projectnessie.versioned.storage.mongodb2.MongoDB2BackendFactory",
    // from nessie-versioned-storage-cache
    "org.projectnessie.versioned.storage.cache.PersistCaches",
    // from nessie-versioned-storage-testextension
    "org.projectnessie.versioned.storage.testextension.PersistExtension",
    // from nessie-client
    "org.projectnessie.client.api.NessieApi"
  })
  void checkClass(String className) throws Exception {
    Class.forName(className);
  }
}
