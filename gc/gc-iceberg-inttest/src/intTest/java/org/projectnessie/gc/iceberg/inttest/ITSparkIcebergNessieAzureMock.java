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
package org.projectnessie.gc.iceberg.inttest;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.objectstoragemock.ObjectStorageMock.MockServer;
import org.projectnessie.storage.uri.StorageUri;

@Disabled("Needs an Iceberg release with https://github.com/apache/iceberg/pull/10045")
public class ITSparkIcebergNessieAzureMock extends AbstractITSparkIcebergNessieObjectStorage {

  private static final String FILESYSTEM = "filesystem1";
  private static final String ACCOUNT = "account123";
  private static final String ACCOUNT_FQ = ACCOUNT + ".dfs.core.windows.net";
  private static final String SECRET = "s3cr3t";
  private static final String SECRET_BASE_64 =
      new String(Base64.getEncoder().encode(SECRET.getBytes(UTF_8)));
  private static final String ADLS_WAREHOUSE_LOCATION =
      "abfs://" + FILESYSTEM + "@" + ACCOUNT_FQ + "/warehouse";

  private static MockServer server;
  private static URI endpoint;

  @Override
  Storage storage() {
    return Storage.ADLS;
  }

  @BeforeAll
  static void beforeAll() {
    HeapStorageBucket bucket = HeapStorageBucket.newHeapStorageBucket();
    server = ObjectStorageMock.builder().putBuckets(FILESYSTEM, bucket.bucket()).build().start();
    endpoint = server.getAdlsGen2BaseUri().resolve(FILESYSTEM);
  }

  @AfterAll
  static void afterAll() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  @Override
  protected StorageUri bucketUri() {
    return StorageUri.of(ADLS_WAREHOUSE_LOCATION);
  }

  @Override
  protected String warehouseURI() {
    return bucketUri().location();
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    Map<String, String> r = new HashMap<>();
    r.put("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore");
    r.put("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
    r.put("fs.azure.always.use.https", "false");
    r.put("fs.azure.abfs.endpoint", endpoint.getHost() + ":" + endpoint.getPort());
    r.put("fs.azure.test.emulator", "true");
    r.put("fs.azure.storage.emulator.account.name", ACCOUNT);
    r.put("fs.azure.account.auth.type", "SharedKey");
    r.put("fs.azure.account.key." + ACCOUNT_FQ, SECRET_BASE_64);
    return r;
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put("io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
    r.put("adls.connection-string." + ACCOUNT_FQ, endpoint.toString());
    r.put("adls.auth.shared-key.account.name", ACCOUNT);
    r.put("adls.auth.shared-key.account.key", SECRET_BASE_64);
    return r;
  }

  @Override
  IcebergFiles icebergFiles() {
    Configuration conf = new Configuration();
    sparkHadoop().forEach(conf::set);
    return IcebergFiles.builder().properties(nessieParams()).hadoopConfiguration(conf).build();
  }
}
