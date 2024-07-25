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
package org.projectnessie.quarkus.config;

import io.smallrye.config.WithConverter;
import io.smallrye.config.WithName;
import java.util.Map;
import org.projectnessie.catalog.service.config.WarehouseConfig;

public interface QuarkusWarehouseConfig extends WarehouseConfig {
  @Override
  @WithName("iceberg-config-defaults")
  Map<String, String> icebergConfigDefaults();

  @Override
  @WithName("iceberg-config-overrides")
  Map<String, String> icebergConfigOverrides();

  @Override
  @WithName("location")
  @WithConverter(TrimTrailingSlash.class)
  String location();
}
