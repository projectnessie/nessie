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
package com.dremio.nessie.server.providers;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.dremio.nessie.server.config.ApplicationConfig;
import com.dremio.nessie.server.config.ApplicationConfig.VersionStoreJdbcConfig;
import com.dremio.nessie.versioned.store.jdbc.DatabaseAdapter;

/**
 * Default bean factory for {@link DatabaseAdapter}, which delegates to
 * {@link DatabaseAdapter#create(String)}. When using a "special" {@link DatabaseAdapter}
 * implementation like Oracle, pull in the Maven artifact into the Quarkus (native image)
 * build. The "special" {@link DatabaseAdapter} implementation must be annotated with
 * {@link javax.enterprise.inject.Alternative @Alternative}.
 */
@Singleton
public class DatabaseAdapterFactory {

  private final ApplicationConfig config;

  @Inject
  public DatabaseAdapterFactory(ApplicationConfig config) {
    this.config = config;
  }

  @Produces
  @Singleton
  public DatabaseAdapter createDatabaseAdapter() {
    VersionStoreJdbcConfig jdbcConfig = config.getVersionStoreJdbcConfig();
    return DatabaseAdapter.create(jdbcConfig.getDatabaseAdapter());
  }
}
