/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.spark.extensions;

import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

public class ITNessieStatementsViaIcebergRest extends AbstractNessieSparkSqlExtensionTest {
  private static ObjectStorageMock.MockServer objectStorage;
  private static HeapStorageBucket bucket;

  @BeforeAll
  public static void start() throws Exception {
    bucket = newHeapStorageBucket();
    objectStorage =
        ObjectStorageMock.builder().putBuckets("bucket", bucket.bucket()).build().start();

    NessieProcess.start(
        "-Dnessie.catalog.default-warehouse=warehouse",
        "-Dnessie.catalog.warehouses.warehouse.location=s3://bucket/",
        "-Dnessie.catalog.service.s3.default-options.endpoint="
            + objectStorage.getS3BaseUri().toString(),
        "-Dnessie.catalog.service.s3.default-options.path-style-access=true",
        "-Dnessie.catalog.service.s3.default-options.region=eu-central-1",
        "-Dnessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:nessie-catalog-secrets.s3-access-key",
        "-Dnessie-catalog-secrets.s3-access-key.name=accessKey",
        "-Dnessie-catalog-secrets.s3-access-key.secret=secretKey");
  }

  @BeforeEach
  public void clearBucket() {
    bucket.clear();
  }

  @AfterAll
  public static void stop() throws Exception {
    objectStorage.close();
    NessieProcess.stop();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    r.putAll(objectStorage.icebergProperties());
    r.remove("http-client.type");
    return r;
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    Map<String, String> iceberg = objectStorage.icebergProperties();
    Map<String, String> r = new HashMap<>();
    r.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    r.put("fs.s3a.access.key", iceberg.get("s3.access-key-id"));
    r.put("fs.s3a.secret.key", iceberg.get("s3.secret-access-key"));
    r.put("fs.s3a.endpoint", iceberg.get("s3.endpoint"));
    return r;
  }

  @Override
  protected boolean useIcebergREST() {
    return true;
  }

  @Override
  protected String warehouseURI() {
    return "s3://bucket/";
  }

  @Override
  protected String nessieApiUri() {
    return NessieProcess.baseUri + "api/v2";
  }

  @Override
  protected String icebergApiUri() {
    return NessieProcess.baseUri + "iceberg/";
  }
}
