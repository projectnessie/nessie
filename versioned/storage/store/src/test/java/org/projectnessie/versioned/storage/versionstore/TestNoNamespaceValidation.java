/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.versionstore;

import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.tests.AbstractNoNamespaceValidation;

@ExtendWith(PersistExtension.class)
public class TestNoNamespaceValidation extends AbstractNoNamespaceValidation {
  @NessiePersist
  @NessieStoreConfig(name = StoreConfig.CONFIG_NAMESPACE_VALIDATION, value = "false")
  protected static Persist persist;

  @Override
  protected VersionStore store() {
    return new VersionStoreImpl(persist);
  }
}
