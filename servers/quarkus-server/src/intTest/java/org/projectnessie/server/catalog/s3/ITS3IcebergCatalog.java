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
package org.projectnessie.server.catalog.s3;

import static java.util.Collections.singletonMap;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.projectnessie.minio.MinioContainer;
import org.projectnessie.server.catalog.AbstractIcebergCatalogIntTests;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;

@WithTestResource(MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
public class ITS3IcebergCatalog extends AbstractIcebergCatalogIntTests {

  @SuppressWarnings("unused")
  // Injected by MinioTestResourceLifecycleManager
  private MinioContainer minio;

  @Override
  protected Map<String, String> catalogOptions() {
    return singletonMap(
        CatalogProperties.WAREHOUSE_LOCATION, minio.s3BucketUri(scheme(), "").toString());
  }

  @Override
  protected String temporaryLocation() {
    return minio.s3BucketUri(scheme(), "/temp/" + UUID.randomUUID()).toString();
  }

  @Override
  protected FileIO temporaryFileIO(RESTCatalog catalog, FileIO io) {
    String ioImpl =
        catalog
            .properties()
            .getOrDefault(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    Map<String, String> props = new HashMap<>(io.properties());
    props.put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "false");
    props.put(S3FileIOProperties.ACCESS_KEY_ID, minio.accessKey());
    props.put(S3FileIOProperties.SECRET_ACCESS_KEY, minio.secretKey());
    return CatalogUtil.loadFileIO(ioImpl, props, new Configuration(false));
  }

  @Override
  protected String scheme() {
    return "s3";
  }
}
