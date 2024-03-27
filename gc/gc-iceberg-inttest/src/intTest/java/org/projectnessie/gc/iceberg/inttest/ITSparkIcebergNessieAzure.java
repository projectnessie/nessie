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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.testing.azurite.Azurite;
import org.projectnessie.testing.azurite.AzuriteAccess;
import org.projectnessie.testing.azurite.AzuriteExtension;

@Disabled("Needs an Iceberg release with https://github.com/apache/iceberg/pull/10045")
@ExtendWith(AzuriteExtension.class)
public class ITSparkIcebergNessieAzure extends AbstractITSparkIcebergNessieObjectStorage {

  public static final String BUCKET_URI = "/my/prefix";

  private static @Azurite AzuriteAccess azuriteAccess;

  @Override
  protected String warehouseURI() {
    return azuriteAccess.location(BUCKET_URI);
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    return azuriteAccess.hadoopConfig();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.putAll(azuriteAccess.icebergProperties());
    return r;
  }

  @Override
  IcebergFiles icebergFiles() {
    Configuration conf = new Configuration();
    azuriteAccess.hadoopConfig().forEach(conf::set);

    return IcebergFiles.builder()
        .properties(azuriteAccess.icebergProperties())
        .hadoopConfiguration(conf)
        .build();
  }

  @Override
  protected URI bucketUri() {
    return URI.create(azuriteAccess.location(BUCKET_URI));
  }
}
