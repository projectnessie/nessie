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

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.projectnessie.catalog.files.config.AdlsConfig;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.files.config.GcsConfig;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.files.config.S3StsCache;
import org.projectnessie.catalog.service.api.LakehouseConfigManagement;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.ImmutableLakehouseConfig;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.ServiceConfig;
import org.projectnessie.catalog.service.config.SmallryeConfigs;
import org.projectnessie.versioned.storage.common.persist.Persist;

public class ConfigProducers {

  @Produces
  @Singleton
  public LakehouseConfigManagement lakehouseConfigProvider(
      SmallryeConfigs smallryeConfigs, Persist persist) {
    boolean dynamic = smallryeConfigs.usePersistedLakehouseConfig();
    return new LakehouseConfigManagementImpl(
        persist, dynamic, staticLakehouseConfig(smallryeConfigs));
  }

  private static LakehouseConfig staticLakehouseConfig(SmallryeConfigs smallryeConfigs) {
    CatalogConfig catalogConfig = smallryeConfigs.catalog().validate();
    AdlsOptions adlsOptions = smallryeConfigs.adls().validate().validate();
    GcsOptions gcsOptions = smallryeConfigs.gcs().validate();
    S3Options s3Options = smallryeConfigs.s3().validate();

    return ImmutableLakehouseConfig.builder()
        .catalog(catalogConfig.deepClone())
        .s3(s3Options.deepClone())
        .gcs(gcsOptions.deepClone())
        .adls(adlsOptions.deepClone())
        .build();
  }

  @Produces
  @RequestScoped
  public LakehouseConfig lakehouseConfig(LakehouseConfigManagement lakehouseConfigManagement) {
    return lakehouseConfigManagement.currentConfig();
  }

  @Produces
  @Singleton
  public S3StsCache s3sts(SmallryeConfigs smallryeConfigs) {
    return smallryeConfigs.effectiveS3StsCache();
  }

  @Produces
  @Singleton
  public S3Config s3Config(SmallryeConfigs smallryeConfigs) {
    return smallryeConfigs.s3config();
  }

  @Produces
  @Singleton
  public AdlsConfig adlsConfig(SmallryeConfigs smallryeConfigs) {
    return smallryeConfigs.adlsconfig();
  }

  @Produces
  @Singleton
  public GcsConfig gcsConfig(SmallryeConfigs smallryeConfigs) {
    return smallryeConfigs.gcsConfig();
  }

  @Produces
  @Singleton
  public ServiceConfig serviceConfig(SmallryeConfigs smallryeConfigs) {
    return smallryeConfigs.serviceConfig();
  }
}
