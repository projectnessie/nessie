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
package org.projectnessie.versioned.storage.bigtable;

import jakarta.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;

public class BigTableBackendFactory implements BackendFactory<BigTableBackendConfig> {

  public static final String NAME = "BigTable";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public BigTableBackendConfig newConfigInstance() {
    // Note: this method should not be called and will throw because dataClient is not set.
    // BigTableBackendConfig instances cannot be constructed using this method.
    return BigTableBackendConfig.builder().build();
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull BigTableBackendConfig config) {
    return new BigTableBackend(config);
  }
}
