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
package org.projectnessie.catalog.server;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

@ConfigMapping(prefix = "nessie.iceberg")
public interface NessieIcebergRestConfig {

  @WithName("warehouseLocation")
  @WithDefault("file:///tmp/nessie-iceberg-rest-warehouse")
  String warehouseLocation();

  @WithName("fileio.implementation")
  @WithDefault("org.apache.iceberg.io.ResolvingFileIO")
  String fileIoImplementation();

  @WithName("fileio.config")
  Map<String, String> fileIoConfig();

  @WithName("nessie-client")
  Map<String, String> nessieClientConfig();

  @WithName("nessie-client-builder")
  @WithDefault("org.projectnessie.client.http.HttpClientBuilder")
  String nessieClientBuilder();

  @WithName("hadoop")
  Map<String, String> hadoopConfig();

  default Configuration hadoopConf() {
    Configuration conf = new Configuration();
    hadoopConfig().forEach(conf::set);
    return conf;
  }
}
