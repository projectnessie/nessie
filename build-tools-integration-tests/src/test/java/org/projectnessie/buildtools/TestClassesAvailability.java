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
    // from nessie-versioned-persist-inmem / test-jar
    "org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource",
    // from nessie-versioned-persist-rocks
    "org.projectnessie.versioned.persist.rocks.RocksDatabaseAdapterFactory",
    // from nessie-client
    "org.projectnessie.client.api.NessieApi"
  })
  void checkClass(String className) throws Exception {
    Class.forName(className);
  }
}
