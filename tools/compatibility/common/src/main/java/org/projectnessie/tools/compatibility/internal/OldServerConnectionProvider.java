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

import static org.projectnessie.tools.compatibility.internal.Util.withClassLoader;

import java.util.Map;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OldServerConnectionProvider implements CloseableResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(OldServerConnectionProvider.class);

  private final ServerKey serverKey;
  final AutoCloseable connectionProvider;

  OldServerConnectionProvider(ServerKey serverKey, ClassLoader classLoader) {
    try {
      this.serverKey = serverKey;
      LOGGER.info(
          "Creating connection provider for Nessie version {} with {} using {}",
          serverKey.getVersion(),
          serverKey.getDatabaseAdapterName(),
          serverKey.getDatabaseAdapterConfig());
      this.connectionProvider =
          withClassLoader(
              classLoader,
              () ->
                  (AutoCloseable)
                      classLoader
                          .loadClass(
                              "org.projectnessie.tools.compatibility.jersey.DatabaseAdapters")
                          .getMethod("createDatabaseConnectionProvider", String.class, Map.class)
                          .invoke(
                              null,
                              serverKey.getDatabaseAdapterName(),
                              serverKey.getDatabaseAdapterConfig()));
    } catch (Exception e) {
      throw Util.throwUnchecked(e);
    }
  }

  @Override
  public void close() throws Throwable {
    if (connectionProvider != null) {
      LOGGER.info(
          "Closing connection provider for Nessie version {} with {} using {}",
          serverKey.getVersion(),
          serverKey.getDatabaseAdapterName(),
          serverKey.getDatabaseAdapterConfig());
      connectionProvider.close();
    }
  }
}
