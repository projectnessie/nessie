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
import org.junit.jupiter.api.Disabled;
import org.projectnessie.objectstoragemock.ObjectStorageMock.MockServer;
import org.projectnessie.storage.uri.StorageUri;

@Disabled(
    "Requires implementation of the /batch/storage/v1 endpoint in object-storage-mock. "
        + "That consumes a multipart/mixed content, which contains a series of serialized HTTP requests.")
public class TestIcebergGCSFiles extends AbstractFiles {

  @Override
  protected String bucket() {
    return "bucket";
  }

  @Override
  protected Map<String, ? extends String> icebergProperties(MockServer server) {
    Map<String, String> props = new HashMap<>();

    props.put("gcs.project-id", "my-project");
    // MUST NOT end with a trailing slash, otherwise code like
    // com.google.cloud.storage.spi.v1.HttpStorageRpc.DefaultRpcBatch.submit inserts an ambiguous
    // empty path segment ("//").
    String uri = server.getGcsBaseUri().toString();
    uri = uri.substring(0, uri.length() - 1);
    props.put("gcs.service.host", uri);
    props.put("gcs.no-auth", "true");

    return props;
  }

  protected Configuration hadoopConfiguration(MockServer server) {
    Configuration conf = new Configuration();

    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    conf.set("fs.gs.project.id", "projectId");
    conf.set("fs.gs.auth.type", "none");

    return conf;
  }

  @Override
  protected StorageUri storageUri(String path) {
    return StorageUri.of(String.format("gs://%s/", bucket())).resolve(path);
  }
}
