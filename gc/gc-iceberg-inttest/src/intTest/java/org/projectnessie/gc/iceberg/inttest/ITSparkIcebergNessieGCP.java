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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.testing.gcs.Gcs;
import org.projectnessie.testing.gcs.GcsAccess;
import org.projectnessie.testing.gcs.GcsExtension;

@ExtendWith(GcsExtension.class)
public class ITSparkIcebergNessieGCP extends AbstractITSparkIcebergNessieObjectStorage {

  public static final String BUCKET_URI = "/my/prefix";

  private static @Gcs GcsAccess gcsAccess;

  @Override
  Storage storage() {
    return Storage.GCS;
  }

  @Override
  protected String warehouseURI() {
    return gcsAccess.bucketUri(BUCKET_URI).toString();
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    return gcsAccess.hadoopConfig();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.putAll(gcsAccess.icebergProperties());
    return r;
  }

  @Override
  IcebergFiles icebergFiles() {
    Configuration conf = new Configuration();
    gcsAccess.hadoopConfig().forEach(conf::set);

    return IcebergFiles.builder()
        .properties(gcsAccess.icebergProperties())
        .hadoopConfiguration(conf)
        .build();
  }

  @Override
  protected StorageUri bucketUri() {
    return StorageUri.of(gcsAccess.bucketUri(BUCKET_URI));
  }
}
