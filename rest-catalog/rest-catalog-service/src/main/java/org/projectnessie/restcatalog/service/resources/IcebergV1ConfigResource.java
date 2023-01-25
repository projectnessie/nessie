/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.restcatalog.service.resources;

import org.apache.iceberg.rest.responses.ConfigResponse;
import org.projectnessie.restcatalog.api.IcebergV1Config;
import org.projectnessie.restcatalog.service.Warehouse;

public class IcebergV1ConfigResource extends BaseIcebergResource implements IcebergV1Config {

  @Override
  public ConfigResponse getConfig(String warehouse) {
    Warehouse w = tenantSpecific.getWarehouse(warehouse);

    ConfigResponse.Builder config = ConfigResponse.builder();

    config.withDefaults(w.configDefaults());
    config.withOverrides(w.configOverrides());

    return config.build();
  }
}
