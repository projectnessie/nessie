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
 * @param <CONFIG> the configuration-options type used to configure database-adapters produced by
 *     this factory.
 */
public interface DatabaseAdapterFactory<CONFIG, CONNECTOR> {
  Builder<CONFIG, CONNECTOR> newBuilder();

  String getName();

  abstract class Builder<CONFIG, CONNECTOR> {
    private CONFIG config;
    private CONNECTOR connector;

    public Builder<CONFIG, CONNECTOR> withConfig(CONFIG config) {
      this.config = config;
      return this;
    }

    public Builder<CONFIG, CONNECTOR> withConnector(CONNECTOR connector) {
      this.connector = connector;
      return this;
    }

    protected abstract CONFIG getDefaultConfig();

    public CONFIG getConfig() {
      if (config == null) {
        config = getDefaultConfig();
      }
      return config;
    }

    public CONNECTOR getConnector() {
      return connector;
    }

    public abstract DatabaseAdapter build();

    public Builder<CONFIG, CONNECTOR> configure(Function<CONFIG, CONFIG> configurator) {
      this.config = configurator.apply(getConfig());
      return this;
    }
  }

  static <CONFIG, CONNECTOR> DatabaseAdapterFactory<CONFIG, CONNECTOR> loadFactoryByName(
      String name) {
    return loadFactory(f -> f.getName().equalsIgnoreCase(name));
  }

  static <CONFIG, CONNECTOR> DatabaseAdapterFactory<CONFIG, CONNECTOR> loadFactory(
      Predicate<DatabaseAdapterFactory<?, ?>> check) {
    @SuppressWarnings("rawtypes")
    Iterator<DatabaseAdapterFactory> iter =
        ServiceLoader.load(DatabaseAdapterFactory.class).iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("No DatabaseAdapterFactory implementation available.");
    }

    while (iter.hasNext()) {
      @SuppressWarnings("unchecked")
      DatabaseAdapterFactory<CONFIG, CONNECTOR> candidate = iter.next();
      if (check.test(candidate)) {
        return candidate;
      }
    }

    throw new IllegalArgumentException("No DatabaseAdapterFactory passed the given predicate.");
  }
}
