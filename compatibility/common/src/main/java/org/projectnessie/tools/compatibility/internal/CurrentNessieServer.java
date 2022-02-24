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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;
import static org.projectnessie.tools.compatibility.internal.Util.throwUnchecked;
import static org.projectnessie.tools.compatibility.jersey.DatabaseAdapters.createDatabaseAdapter;
import static org.projectnessie.tools.compatibility.jersey.DatabaseAdapters.createDatabaseConnectionProvider;
import static org.projectnessie.tools.compatibility.jersey.ServerConfigExtension.SERVER_CONFIG;

import java.net.URI;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.jersey.JerseyServer;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CurrentNessieServer implements NessieServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CurrentNessieServer.class);

  private final ServerKey serverKey;
  private final JerseyServer jersey;
  private final BooleanSupplier initializeRepository;

  private DatabaseAdapter databaseAdapter;
  private DatabaseConnectionProvider<DatabaseConnectionConfig> connectionProvider;

  static CurrentNessieServer currentNessieServer(
      ExtensionContext extensionContext,
      ServerKey serverKey,
      BooleanSupplier initializeRepository) {
    return extensionStore(extensionContext)
        .getOrComputeIfAbsent(
            serverKey,
            k -> new CurrentNessieServer(serverKey, initializeRepository),
            CurrentNessieServer.class);
  }

  @Override
  public URI getUri() {
    return jersey.getUri();
  }

  @Override
  public void close() throws Throwable {
    try {
      if (jersey != null) {
        jersey.close();
      }
    } finally {
      if (connectionProvider != null) {
        LOGGER.info("Closing connection provider for current Nessie version");
        connectionProvider.close();
      }
    }
  }

  private CurrentNessieServer(ServerKey serverKey, BooleanSupplier initializeRepository) {
    try {
      this.serverKey = serverKey;
      this.initializeRepository = initializeRepository;
      this.jersey = new JerseyServer(this::getOrCreateDatabaseAdapter);

      LOGGER.info("Nessie Server started at {}", jersey.getUri());
    } catch (Exception e) {
      throw throwUnchecked(e);
    }
  }

  private synchronized DatabaseAdapter getOrCreateDatabaseAdapter() {
    if (this.databaseAdapter == null) {
      LOGGER.info("Creating connection provider for current Nessie version");
      this.connectionProvider =
          createDatabaseConnectionProvider(
              serverKey.getDatabaseAdapterName(), serverKey.getDatabaseAdapterConfig());
      this.databaseAdapter =
          createDatabaseAdapter(serverKey.getDatabaseAdapterName(), connectionProvider);

      if (initializeRepository.getAsBoolean()) {
        databaseAdapter.eraseRepo();
        databaseAdapter.initializeRepo(SERVER_CONFIG.getDefaultBranch());
      }
    }
    return this.databaseAdapter;
  }
}
