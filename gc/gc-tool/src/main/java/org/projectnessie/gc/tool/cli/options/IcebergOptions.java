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
      description = {
        "Iceberg properties used to configure the FileIO.",
        "The following properties are almost always required.",
        "",
        "For S3:",
        "- s3.access-key-id",
        "- s3.secret-access-key",
        "For non-AWS S3 you need to specify the endpoint and possibly enable path-style-access:",
        "- s3.endpoint",
        "- s3.path-style-access=true",
        "",
        "For GCS:",
        "- io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO",
        "- gcs.project-id",
        "- gcs.oauth2.token",
        "",
        "For ADLS:",
        "- io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO",
        "- adls.auth.shared-key.account.name",
        "- adls.auth.shared-key.account.key",
      })
  Map<String, String> icebergProperties = new HashMap<>();

  @CommandLine.Option(
      names = {"-H", "--hadoop"},
      split = ",",
      description = {
        "Hadoop configuration option, required when using an Iceberg FileIO that is not S3FileIO.",
        "The following configuration settings might be required.",
        "",
        "For S3:",
        "- fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "- fs.s3a.access.key",
        "- fs.s3a.secret.key",
        "- fs.s3a.endpoint, if you use an S3 compatible object store like MinIO",
        "",
        "For GCS:",
        "- fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "- fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "- fs.gs.project.id",
        "- fs.gs.auth.type=USER_CREDENTIALS",
        "- fs.gs.auth.client.id",
        "- fs.gs.auth.client.secret",
        "- fs.gs.auth.refresh.token",
        "",
        "For ADLS:",
        "- fs.azure.impl=org.apache.hadoop.fs.azure.AzureNativeFileSystemStore",
        "- fs.AbstractFileSystem.azure.impl=org.apache.hadoop.fs.azurebfs.Abfs",
        "- fs.azure.storage.emulator.account.name",
        "- fs.azure.account.auth.type=SharedKey",
        "- fs.azure.account.key.<account>=<base-64-encoded-secret>",
      })
  Map<String, String> hadoopConf = new HashMap<>();

  public Map<String, String> getIcebergProperties() {
    return icebergProperties;
  }

  public Map<String, String> getHadoopConf() {
    return hadoopConf;
  }
}
