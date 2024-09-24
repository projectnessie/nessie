/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.api;

import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.SmallryeConfigs;

/**
 * Management service to get and update {@link LakehouseConfig}.
 *
 * <p>The implementation allow "managing" <em>static</em> configuration (retrieved from the Quarkus
 * configuration) and <em>dynamic</em> configuration, which is retrieved and persisted by Nessie.
 * The static vs dynamic behavior is controlled by {@link
 * SmallryeConfigs#usePersistedLakehouseConfig()}.
 *
 * <p>A migration from managing a static configuration to a dynamically maintained one will be added
 * later.
 */
public interface LakehouseConfigManagement {
  LakehouseConfig currentConfig();

  void updateConfig(LakehouseConfig config, LakehouseConfig expected);
}
