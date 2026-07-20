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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.storage.uri.StorageUri;
import org.projectnessie.testing.floci.az.FlociAz;
import org.projectnessie.testing.floci.az.FlociAzAccess;
import org.projectnessie.testing.floci.az.FlociAzExtension;

@Disabled(
    "1) Needs an Iceberg release with https://github.com/apache/iceberg/pull/10045 "
        + "2) FlociAz is incompatible with ADLS v2 list-prefix REST endpoint")
@ExtendWith(FlociAzExtension.class)
public class ITSparkIcebergNessieAzure extends AbstractITSparkIcebergNessieObjectStorage {

  public static final String BUCKET_URI = "/my/prefix";

  private static @FlociAz FlociAzAccess flociAzAccess;

  @Override
  Storage storage() {
    return Storage.ADLS;
  }

  @Override
  protected String warehouseURI() {
    return flociAzAccess.location(BUCKET_URI);
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    return flociAzAccess.hadoopConfig();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.putAll(flociAzAccess.icebergProperties());
    return r;
  }

  @Override
  IcebergFiles icebergFiles() {
    Configuration conf = new Configuration();
    flociAzAccess.hadoopConfig().forEach(conf::set);

    return IcebergFiles.builder()
        .properties(flociAzAccess.icebergProperties())
        .hadoopConfiguration(conf)
        .build();
  }

  @Override
  protected StorageUri bucketUri() {
    return StorageUri.of(flociAzAccess.location(BUCKET_URI));
  }
}
