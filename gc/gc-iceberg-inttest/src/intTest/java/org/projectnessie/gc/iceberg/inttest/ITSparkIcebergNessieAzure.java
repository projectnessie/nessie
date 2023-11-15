/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.iceberg.inttest;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.testing.azurite.AzuriteContainer;

@Disabled(
    "Iceberg-azure cannot use Azurite (emulator), as it does not allow setting a shared-secret (user/pass)")
// org.apache.iceberg.azure.AzureProperties.applyClientConfiguration only allows SAS and default,
// but not
// UsernamePasswordCredential, although even Hadoop-Azure would work with it.
public class ITSparkIcebergNessieAzure extends AbstractITSparkIcebergNessieObjectStorage {

  private static AzuriteContainer azuriteContainer;

  @BeforeAll
  static void startAzurite() {
    azuriteContainer = new AzuriteContainer();
    azuriteContainer.start();
  }

  @AfterAll
  static void stopAzurite() {
    azuriteContainer.stop();
  }

  @BeforeEach
  void createStorageContainer() {
    azuriteContainer.createStorageContainer();
  }

  @AfterEach
  void deleteStorageContainer() {
    azuriteContainer.deleteStorageContainer();
  }

  @Override
  protected String warehouseURI() {
    return azuriteContainer.location("");
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    return azuriteContainer.hadoopConfig();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
    r.put(AzureProperties.ADLS_CONNECTION_STRING_PREFIX, azuriteContainer.endpoint());
    return r;
  }

  @Override
  IcebergFiles icebergFiles() {
    Map<String, String> props = new HashMap<>();

    Configuration conf = new Configuration();

    return IcebergFiles.builder().properties(props).hadoopConfiguration(conf).build();
  }

  @Override
  protected URI s3BucketUri() {
    return URI.create(azuriteContainer.location(""));
  }
}
