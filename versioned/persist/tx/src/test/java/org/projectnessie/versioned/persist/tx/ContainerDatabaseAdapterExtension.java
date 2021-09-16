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
package org.projectnessie.versioned.persist.tx;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.tests.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tx.local.ImmutableDefaultLocalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tx.local.LocalDatabaseAdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public abstract class ContainerDatabaseAdapterExtension extends DatabaseAdapterExtension {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ContainerDatabaseAdapterExtension.class);

  private static final Namespace NAMESPACE =
      Namespace.create(ContainerDatabaseAdapterExtension.class);
  private static final String KEY = "database-adapter";

  private final String adapterName;

  protected ContainerDatabaseAdapterExtension(String adapterName) {
    this.adapterName = adapterName;
  }

  @Override
  protected DatabaseAdapter createAdapter(ExtensionContext context, TestConfigurer testConfigurer) {
    return createAdapter(
        testConfigurer,
        DatabaseAdapterFactory.<LocalDatabaseAdapterConfig>loadFactoryByName(adapterName)
            .newBuilder()
            .withConfig(ImmutableDefaultLocalDatabaseAdapterConfig.builder().build()),
        config -> configureDatabaseAdapter(context, config));
  }

  private LocalDatabaseAdapterConfig configureDatabaseAdapter(
      ExtensionContext context, LocalDatabaseAdapterConfig config) {
    ContainerHolder container = context.getStore(NAMESPACE).get(KEY, ContainerHolder.class);
    return config
        .withJdbcUrl(container.container.getJdbcUrl())
        .withJdbcUser(container.container.getUsername())
        .withJdbcPass(container.container.getPassword());
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    context
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(KEY, k -> setupContainer(), ContainerHolder.class);
    super.beforeAll(context);
  }

  private ContainerHolder setupContainer() {
    JdbcDatabaseContainer<?> container = createContainer();
    container.withLogConsumer(new Slf4jLogConsumer(LOGGER)).start();
    return new ContainerHolder(container);
  }

  protected abstract JdbcDatabaseContainer<?> createContainer();

  static class ContainerHolder implements CloseableResource {
    final JdbcDatabaseContainer<?> container;

    ContainerHolder(JdbcDatabaseContainer<?> container) {
      this.container = container;
    }

    @Override
    public void close() {
      container.stop();
    }
  }
}
