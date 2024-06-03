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
package org.projectnessie.gc.iceberg.files;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.projectnessie.objectstoragemock.ObjectStorageMock.MockServer;
import org.projectnessie.storage.uri.StorageUri;

public class TestIcebergAdlsFiles extends AbstractFiles {

  @Override
  protected String bucket() {
    return "$root";
  }

  @Override
  protected Map<String, ? extends String> icebergProperties(MockServer server) {
    Map<String, String> props = new HashMap<>();

    props.put("adls.connection-string.account", server.getAdlsGen2BaseUri().toString());
    props.put("adls.auth.shared-key.account.name", "account@account.dfs.core.windows.net");
    props.put("adls.auth.shared-key.account.key", "key");

    return props;
  }

  protected Configuration hadoopConfiguration(MockServer server) {
    Configuration conf = new Configuration();

    conf.set("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore");
    conf.set("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
    conf.set("fs.azure.storage.emulator.account.name", "account");
    conf.set("fs.azure.account.auth.type", "SharedKey");
    conf.set("fs.azure.account.key.account", "<base-64-encoded-secret>");

    return conf;
  }

  @Override
  protected StorageUri storageUri(String path) {
    return StorageUri.of(String.format("abfs://%s@account/", bucket())).resolve(path);
  }
}
