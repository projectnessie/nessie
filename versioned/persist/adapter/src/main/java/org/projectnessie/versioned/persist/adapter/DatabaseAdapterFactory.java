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
package org.projectnessie.versioned.persist.adapter;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Each {@link org.projectnessie.versioned.persist.adapter.DatabaseAdapter} is configured and
 * instantiated by an implementation of this factory.
 *
 * <p>This factory approach is useful to have "generic" infrastructure code for unit/integration
 * tests, (micro)benchmarks but also production-code. See {@code SystemPropertiesConfigurer}.
 *
 * <p>Concrete database-adapter factory implementations can then be easily filtered and loaded via
 * {@link #loadFactory(Predicate)} or {@link #loadFactoryByName(String)}.
 *
 * @param <Config> the configuration-options type used to configure database-adapters produced by
 *     this factory.
 */
public interface DatabaseAdapterFactory<
    Config extends DatabaseAdapterConfig,
    AdjustableConfig extends Config,
    Connector extends DatabaseConnectionProvider<?>> {

  Builder<Config, AdjustableConfig, Connector> newBuilder();

  String getName();

  abstract class Builder<Config, AdjustableConfig, Connector> {
    private Config config;
    private Connector connector;

    public Builder<Config, AdjustableConfig, Connector> withConfig(Config config) {
      this.config = config;
      return this;
    }

    public Builder<Config, AdjustableConfig, Connector> withConnector(Connector connector) {
      this.connector = connector;
      return this;
    }

    protected abstract Config getDefaultConfig();

    protected abstract AdjustableConfig adjustableConfig(Config config);

    public Config getConfig() {
      if (config == null) {
        config = getDefaultConfig();
      }
      return config;
    }

    public Connector getConnector() {
      return connector;
    }

    public abstract DatabaseAdapter build();

    public Builder<Config, AdjustableConfig, Connector> configure(
        Function<AdjustableConfig, Config> configurator) {
      this.config = configurator.apply(adjustableConfig(getConfig()));
      return this;
    }
  }

  static <
          Config extends DatabaseAdapterConfig,
          AdjustableConfig extends Config,
          Connector extends DatabaseConnectionProvider<?>>
      DatabaseAdapterFactory<Config, AdjustableConfig, Connector> loadFactoryByName(String name) {
    return loadFactory(f -> f.getName().equalsIgnoreCase(name));
  }

  static <
          Config extends DatabaseAdapterConfig,
          AdjustableConfig extends Config,
          Connector extends DatabaseConnectionProvider<?>>
      DatabaseAdapterFactory<Config, AdjustableConfig, Connector> loadFactory(
          Predicate<DatabaseAdapterFactory<?, ?, ?>> check) {
    @SuppressWarnings("rawtypes")
    Iterator<DatabaseAdapterFactory> iter =
        ServiceLoader.load(DatabaseAdapterFactory.class).iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("No DatabaseAdapterFactory implementation available.");
    }

    while (iter.hasNext()) {
      @SuppressWarnings("unchecked")
      DatabaseAdapterFactory<Config, AdjustableConfig, Connector> candidate = iter.next();
      if (check.test(candidate)) {
        return candidate;
      }
    }

    throw new IllegalArgumentException("No DatabaseAdapterFactory passed the given predicate.");
  }
}
