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

import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.projectnessie.gc.iceberg.files.IcebergFiles;

@Disabled("There is no official emulator for Google's Cloud Storage")
// There is one emulator though: docker.io/oittaa/gcp-storage-emulator /
// https://github.com/oittaa/gcp-storage-emulator
// That one seems to work, but then GCS authn in Iceberg has no support for "no credentials" - so
// testing is currently impossible - and this test class is unfinished.
public class ITSparkIcebergNessieGCP extends AbstractITSparkIcebergNessieObjectStorage {

  private static Storage gcsService;
  private static String googleProjectId;
  private static String googleServiceHost;

  private static String bucket;
  private static String bucketUri;

  @BeforeAll
  static void setupGcs() {
    googleProjectId = "prj" + ThreadLocalRandom.current().nextInt(100000);
    googleServiceHost = "http://[::1]:8080";

    gcsService =
        StorageOptions.newBuilder()
            .setProjectId(googleProjectId)
            .setHost(googleServiceHost)
            .setCredentials(NoCredentials.getInstance())
            .setRetrySettings(ServiceOptions.getNoRetrySettings())
            .build()
            .getService();

    bucket = "bucket" + ThreadLocalRandom.current().nextInt(100000);
    bucketUri = String.format("gs://%s/", bucket);

    gcsService.create(BucketInfo.of(bucket));
  }

  @AfterAll
  static void stopGcs() throws Exception {
    gcsService.close();
  }

  @Override
  protected String warehouseURI() {
    return bucketUri;
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    Map<String, String> r = new HashMap<>();
    r.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    r.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    return r;
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");
    r.put(GCPProperties.GCS_PROJECT_ID, googleProjectId);
    r.put(GCPProperties.GCS_SERVICE_HOST, googleServiceHost);
    return r;
  }

  @AfterEach
  void purgeGcs() {
    gcsService.list(bucket).iterateAll().forEach(Blob::delete);
  }

  @Override
  IcebergFiles icebergFiles() {
    Map<String, String> props = new HashMap<>();
    //    props.put("s3.access-key-id", accessKey());
    //    props.put("s3.secret-access-key", secretKey());
    //    props.put("s3.endpoint", s3endpoint());
    //    props.put("http-client.type", "urlconnection");

    Configuration conf = new Configuration();
    //    conf.set("fs.s3a.access.key", accessKey());
    //    conf.set("fs.s3a.secret.key", secretKey());
    //    conf.set("fs.s3a.endpoint", s3endpoint());

    return IcebergFiles.builder().properties(props).hadoopConfiguration(conf).build();
  }

  @Override
  protected URI s3BucketUri() {
    return URI.create(bucketUri);
  }
}
