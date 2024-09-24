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
package org.projectnessie.server.catalog;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.UUID;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.service.api.LakehouseConfigManagement;
import org.projectnessie.catalog.service.config.ImmutableCatalogConfig;
import org.projectnessie.catalog.service.config.ImmutableLakehouseConfig;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.objtypes.ImmutableLakehouseConfigObj;
import org.projectnessie.catalog.service.objtypes.LakehouseConfigObj;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Persist;

public class LakehouseConfigManagementImpl implements LakehouseConfigManagement {
  private final Persist persist;
  private final boolean dynamic;
  private final LakehouseConfig staticLakehouseConfig;

  public LakehouseConfigManagementImpl(
      Persist persist, boolean dynamic, LakehouseConfig staticLakehouseConfig) {
    this.persist = persist;
    this.dynamic = dynamic;
    this.staticLakehouseConfig = staticLakehouseConfig;
  }

  @Override
  public LakehouseConfig currentConfig() {
    if (!dynamic) {
      return staticLakehouseConfig;
    }

    try {
      // We could eventually save the persistence round-trip in a followup change.
      LakehouseConfigObj obj = loadCurrent();
      return obj.lakehouseConfig();
    } catch (ObjNotFoundException e) {
      return ImmutableLakehouseConfig.builder()
          .catalog(ImmutableCatalogConfig.builder().build())
          .s3(ImmutableS3Options.builder().build())
          .gcs(ImmutableGcsOptions.builder().build())
          .adls(ImmutableAdlsOptions.builder().build())
          .build();
    }
  }

  private LakehouseConfigObj loadCurrent() throws ObjNotFoundException {
    return persist.fetchTypedObj(
        LakehouseConfigObj.OBJ_ID, LakehouseConfigObj.OBJ_TYPE, LakehouseConfigObj.class);
  }

  @Override
  public void updateConfig(LakehouseConfig config, LakehouseConfig expected) {
    checkState(dynamic, "Cannot update the lakehouse config in Quarkus configuration");

    checkArgument(
        config != null && expected != null, "Updated config and expected must not be null");
    checkArgument(!config.equals(expected), "Updated config and expected config must not be equal");

    ImmutableLakehouseConfigObj newObj =
        LakehouseConfigObj.builder()
            .versionToken(UUID.randomUUID().toString())
            .lakehouseConfig(config)
            .build();

    try {
      while (true) {
        try {
          LakehouseConfigObj existing = loadCurrent();
          checkArgument(
              existing.lakehouseConfig().equals(expected),
              "Currently persisted lakehouse configuration changed, cannot apply changes");

          if (persist.updateConditional(newObj, existing)) {
            // changed obj persisted, we're good
            return;
          }

        } catch (ObjNotFoundException e) {
          if (persist.storeObj(newObj)) {
            // new obj persisted, we're good
            return;
          }
        }
      }
    } catch (ObjTooLargeException e) {
      throw new RuntimeException(e);
    }
  }
}
