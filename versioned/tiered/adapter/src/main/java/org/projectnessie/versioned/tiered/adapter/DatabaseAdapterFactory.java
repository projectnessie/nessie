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
package org.projectnessie.versioned.tiered.adapter;

import java.util.function.Function;

public interface DatabaseAdapterFactory<CONFIG extends DatabaseAdapterConfig> {
  Builder<CONFIG> newBuilder();

  String getName();

  abstract class Builder<CONFIG extends DatabaseAdapterConfig> {
    private CONFIG config;

    public Builder<CONFIG> withConfig(CONFIG config) {
      this.config = config;
      return this;
    }

    protected abstract CONFIG getDefaultConfig();

    protected CONFIG getConfig() {
      if (config == null) {
        config = getDefaultConfig();
      }
      return config;
    }

    public abstract DatabaseAdapter build();

    @SuppressWarnings("unchecked")
    public Builder<CONFIG> configure(Function<CONFIG, DatabaseAdapterConfig> configurator) {
      this.config = (CONFIG) configurator.apply(getConfig());
      return this;
    }
  }
}
