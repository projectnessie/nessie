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

import static org.projectnessie.tools.compatibility.internal.Configurations.backendConfigBuilderApply;
import static org.projectnessie.tools.compatibility.internal.Util.withClassLoader;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OldServerConnectionProvider implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OldServerConnectionProvider.class);

  private final ServerKey serverKey;
  final AutoCloseable connectionProvider;

  OldServerConnectionProvider(
      ServerKey serverKey, ClassLoader classLoader, Consumer<Object> backendConfigConsumer) {
    try {
      this.serverKey = serverKey;
      LOGGER.info(
          "Creating connection provider for Nessie version {} with {} using {}",
          serverKey.getVersion(),
          serverKey.getStorageName(),
          serverKey.getConfig());
      this.connectionProvider = createBackend(classLoader, serverKey, backendConfigConsumer);
    } catch (Exception e) {
      throw Util.throwUnchecked(e);
    }
  }

  /**
   * Need to build the backend configuration and the backend here using "good old reflection",
   * because the originally intended way to use {@code BackendFactory.newConfigInstance()} does not
   * work: for example {@code RocksDBBackendConfig} requires the {@code databasePath} attribute, but
   * there is no way to create a database-config instance using supplied configuration properties.
   */
  static AutoCloseable createBackend(
      ClassLoader classLoader, ServerKey serverKey, Consumer<Object> backendConfigConsumer) {
    try {
      Object backendFactory =
          withClassLoader(
              classLoader,
              () ->
                  classLoader
                      .loadClass("org.projectnessie.versioned.storage.common.persist.PersistLoader")
                      .getMethod("findFactoryByName", String.class)
                      .invoke(null, serverKey.getStorageName()));

      Object backendConfigBuilder =
          withClassLoader(
              classLoader,
              () ->
                  classLoader
                      .loadClass(
                          backendFactory
                              .getClass()
                              .getName()
                              .replace("BackendFactory", "BackendConfig"))
                      .getDeclaredMethod("builder")
                      .invoke(null));

      backendConfigBuilderApply(
          backendConfigBuilder.getClass(), backendConfigBuilder, serverKey.getConfig()::get);

      return withClassLoader(
          classLoader,
          () -> {
            backendConfigConsumer.accept(backendConfigBuilder);

            Object backendConfig =
                backendConfigBuilder.getClass().getMethod("build").invoke(backendConfigBuilder);

            AutoCloseable backend =
                (AutoCloseable)
                    backendFactory
                        .getClass()
                        .getMethod("buildBackend", Object.class)
                        .invoke(backendFactory, backendConfig);

            classLoader
                .loadClass("org.projectnessie.versioned.storage.common.persist.Backend")
                .getMethod("setupSchema")
                .invoke(backend);

            return backend;
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    if (connectionProvider != null) {
      LOGGER.info(
          "Closing connection provider for Nessie version {} with {} using {}",
          serverKey.getVersion(),
          serverKey.getStorageName(),
          serverKey.getConfig());
      connectionProvider.close();
    }
  }
}
