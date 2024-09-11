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

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.projectnessie.catalog.files.config.AdlsConfig;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.ImmutableLakehouseConfig;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.catalog.service.config.ServiceConfig;
import org.projectnessie.catalog.service.config.SmallryeConfigs;

public class ConfigProducers {

  @Produces
  @Singleton
  public LakehouseConfig lakehouseConfig(SmallryeConfigs smallryeConfigs) {
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
  @Singleton
  public CatalogConfig catalogConfig(LakehouseConfig lakehouseConfig) {
    return lakehouseConfig.catalog();
  }

  @Produces
  @Singleton
  public S3Options s3Options(LakehouseConfig lakehouseConfig) {
    return lakehouseConfig.s3();
  }

  @Produces
  @Singleton
  public GcsOptions gcsOptions(LakehouseConfig lakehouseConfig) {
    return lakehouseConfig.gcs();
  }

  @Produces
  @Singleton
  public AdlsOptions adlsOptions(LakehouseConfig lakehouseConfig) {
    return lakehouseConfig.adls();
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
  public ServiceConfig serviceConfig(SmallryeConfigs smallryeConfigs) {
    return smallryeConfigs.serviceConfig();
  }
}
