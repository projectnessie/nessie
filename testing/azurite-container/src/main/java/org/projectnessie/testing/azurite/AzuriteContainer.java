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
package org.projectnessie.testing.azurite;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

public class AzuriteContainer extends GenericContainer<AzuriteContainer> {

  private static final int DEFAULT_PORT = 10000; // default blob service port
  private static final String DEFAULT_IMAGE = "mcr.microsoft.com/azure-storage/azurite";
  private static final String DEFAULT_TAG = "latest";
  private static final String LOG_WAIT_REGEX =
      "Azurite Blob service is successfully listening at .*";

  public static final String ACCOUNT = "account";
  public static final String ACCOUNT_FQ = ACCOUNT + ".dfs.core.windows.net";
  public static final String KEY = "key";
  public static final String KEY_BASE64 =
      new String(Base64.getEncoder().encode(KEY.getBytes(StandardCharsets.UTF_8)));
  ;
  public static final String STORAGE_CONTAINER = "container";

  public AzuriteContainer() {
    this(DEFAULT_IMAGE + ":" + DEFAULT_TAG);
  }

  public AzuriteContainer(String image) {
    super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
    this.addExposedPort(DEFAULT_PORT);
    this.addEnv("AZURITE_ACCOUNTS", ACCOUNT + ":" + KEY_BASE64);
    this.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(LOG_WAIT_REGEX));
  }

  public void createStorageContainer() {
    serviceClient().createFileSystem(STORAGE_CONTAINER);
  }

  public void deleteStorageContainer() {
    serviceClient().deleteFileSystem(STORAGE_CONTAINER);
  }

  public DataLakeServiceClient serviceClient() {
    return new DataLakeServiceClientBuilder()
        .endpoint(endpoint())
        .credential(credential())
        .buildClient();
  }

  public String location(String path) {
    return String.format("abfs://%s@%s/%s", STORAGE_CONTAINER, ACCOUNT_FQ, path);
  }

  public String endpoint() {
    return String.format("http://%s/%s", endpointHostPort(), ACCOUNT);
  }

  public String endpointHostPort() {
    return String.format("%s:%d", getHost(), getMappedPort(DEFAULT_PORT));
  }

  public StorageSharedKeyCredential credential() {
    return new StorageSharedKeyCredential(ACCOUNT, KEY_BASE64);
  }

  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();

    r.put("fs.azure.impl", "org.apache.hadoop.fs.azure.AzureNativeFileSystemStore");
    r.put("fs.AbstractFileSystem.azure.impl", "org.apache.hadoop.fs.azurebfs.Abfs");

    r.put("fs.azure.always.use.https", "false");
    r.put("fs.azure.abfs.endpoint", endpointHostPort());

    r.put("fs.azure.account.auth.type", "SharedKey");
    r.put("fs.azure.storage.emulator.account.name", ACCOUNT_FQ);
    r.put("fs.azure.account.key." + ACCOUNT_FQ, KEY_BASE64);

    return r;
  }
}
