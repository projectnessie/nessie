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
package org.projectnessie.catalog.files.s3;

import java.time.Clock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.http.SdkHttpClient;

public class TestS3Clients extends AbstractClients {

  private static SdkHttpClient sdkHttpClient;

  @BeforeAll
  static void createHttpClient() {
    S3Config s3Config = S3Config.builder().build();
    sdkHttpClient = S3Clients.apacheHttpClient(s3Config);
  }

  @AfterAll
  static void closeHttpClient() {
    if (sdkHttpClient != null) {
      sdkHttpClient.close();
    }
  }

  @Override
  protected ObjectIO buildObjectIO(
      ObjectStorageMock.MockServer server1, ObjectStorageMock.MockServer server2) {

    S3ProgrammaticOptions.Builder s3options =
        S3ProgrammaticOptions.builder()
            .cloud(Cloud.PRIVATE)
            .putBuckets(
                BUCKET_1,
                S3ProgrammaticOptions.S3PerBucketOptions.builder()
                    .endpoint(server1.getS3BaseUri())
                    .region("us-west-1")
                    .accessKeyIdRef("ak1")
                    .secretAccessKeyRef("sak1")
                    .accessPoint(BUCKET_1)
                    .build());
    if (server2 != null) {
      s3options.putBuckets(
          BUCKET_2,
          S3ProgrammaticOptions.S3PerBucketOptions.builder()
              .endpoint(server2.getS3BaseUri())
              .region("eu-central-2")
              .accessKeyIdRef("ak2")
              .secretAccessKeyRef("sak2")
              .build());
    }

    S3ClientSupplier supplier =
        new S3ClientSupplier(
            sdkHttpClient, S3Config.builder().build(), s3options.build(), secret -> "secret", null);
    return new S3ObjectIO(supplier, Clock.systemUTC());
  }

  @Override
  protected StorageUri buildURI(String bucket, String key) {
    return StorageUri.of(String.format("s3://%s/%s", bucket, key));
  }
}
