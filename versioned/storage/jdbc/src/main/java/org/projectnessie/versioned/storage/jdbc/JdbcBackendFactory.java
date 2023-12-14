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
package org.projectnessie.versioned.storage.jdbc;

import jakarta.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;

public class JdbcBackendFactory implements BackendFactory<JdbcBackendConfig> {

  public static final String NAME = "JDBC";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public JdbcBackendConfig newConfigInstance() {
    return JdbcBackendConfig.builder().build();
  }

  @Override
  @Nonnull
  public JdbcBackend buildBackend(@Nonnull JdbcBackendConfig config) {
    DatabaseSpecific databaseSpecific = DatabaseSpecifics.detect(config.dataSource());
    return new JdbcBackend(config, databaseSpecific, false);
  }
}
