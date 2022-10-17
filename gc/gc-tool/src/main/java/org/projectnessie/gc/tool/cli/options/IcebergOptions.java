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
package org.projectnessie.gc.tool.cli.options;

import java.util.HashMap;
import java.util.Map;
import picocli.CommandLine;

public class IcebergOptions {

  @CommandLine.Option(
      names = {"-I", "--iceberg"},
      split = ",",
      description = "Iceberg properties used to configure the FileIO.")
  Map<String, String> icebergProperties = new HashMap<>();

  @CommandLine.Option(
      names = {"-H", "--hadoop"},
      split = ",",
      description =
          "Hadoop configuration option, required when using an Iceberg FileIO that is not S3.")
  Map<String, String> hadoopConf = new HashMap<>();

  public Map<String, String> getIcebergProperties() {
    return icebergProperties;
  }

  public Map<String, String> getHadoopConf() {
    return hadoopConf;
  }
}
