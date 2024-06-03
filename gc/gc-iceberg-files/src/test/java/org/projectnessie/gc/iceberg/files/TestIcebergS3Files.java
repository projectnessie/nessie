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

public class TestIcebergS3Files extends AbstractFiles {

  @Override
  protected String bucket() {
    return "bucket";
  }

  @Override
  protected Map<String, ? extends String> icebergProperties(MockServer server) {
    Map<String, String> props = new HashMap<>();

    props.put("s3.access-key-id", "accessKey");
    props.put("s3.secret-access-key", "secretKey");
    props.put("s3.endpoint", server.getS3BaseUri().toString());
    // must enforce path-style access because S3Resource has the bucket name in its path
    props.put("s3.path-style-access", "true");
    props.put("http-client.type", "urlconnection");

    return props;
  }

  protected Configuration hadoopConfiguration(MockServer server) {
    Configuration conf = new Configuration();

    conf.set("fs.s3a.access.key", "accessKey");
    conf.set("fs.s3a.secret.key", "secretKey");
    conf.set("fs.s3a.endpoint", server.getS3BaseUri().toString());

    return conf;
  }

  @Override
  protected StorageUri storageUri(String path) {
    return StorageUri.of(String.format("s3://%s/", bucket())).resolve(path);
  }
}
