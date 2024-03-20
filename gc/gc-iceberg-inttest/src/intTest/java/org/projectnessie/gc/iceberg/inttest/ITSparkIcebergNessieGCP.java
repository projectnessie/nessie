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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.testing.gcs.GCSContainer;

public class ITSparkIcebergNessieGCP extends AbstractITSparkIcebergNessieObjectStorage {

  private static GCSContainer gcsContainer;
  private static Storage gcsService;

  @BeforeAll
  static void setupGcs() {
    gcsContainer = new GCSContainer();
    gcsContainer.start();

    gcsService = gcsContainer.newStorage();
  }

  @AfterAll
  static void stopGcs() throws Exception {
    gcsService.close();
  }

  @Override
  protected String warehouseURI() {
    return gcsContainer.bucketUri();
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    return gcsContainer.hadoopConfig();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.putAll(gcsContainer.icebergProperties());
    return r;
  }

  @AfterEach
  void purgeGcs() {
    gcsService.list(gcsContainer.bucket()).iterateAll().forEach(Blob::delete);
  }

  @Override
  IcebergFiles icebergFiles() {
    Map<String, String> props = gcsContainer.icebergProperties();

    Configuration conf = new Configuration();
    gcsContainer.hadoopConfig().forEach(conf::set);

    return IcebergFiles.builder().properties(props).hadoopConfiguration(conf).build();
  }

  @Override
  protected URI s3BucketUri() {
    return URI.create(gcsContainer.bucketUri());
  }
}
