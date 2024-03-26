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
package org.projectnessie.minio;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@ExtendWith({MinioExtension.class, SoftAssertionsExtension.class})
public class ITMinioExtension {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void smokeTest(
      @Minio(accessKey = "myaccesskey", secretKey = "mysecretkey", bucket = "mybucket")
          MinioAccess minio,
      @TempDir Path dir)
      throws Exception {
    soft.assertThat(minio.hostPort()).isNotEmpty();
    soft.assertThat(minio.s3endpoint()).isNotEmpty().startsWith("http");

    soft.assertThat(minio.bucket()).isNotEmpty().isEqualTo("mybucket");
    soft.assertThat(minio.accessKey()).isNotEmpty().isEqualTo("myaccesskey");
    soft.assertThat(minio.secretKey()).isNotEmpty().isEqualTo("mysecretkey");

    soft.assertThat(minio.icebergProperties())
        .containsEntry("s3.access-key-id", minio.accessKey())
        .containsEntry("s3.secret-access-key", minio.secretKey())
        .containsEntry("s3.endpoint", minio.s3endpoint())
        .containsKey("http-client.type");

    soft.assertThat(minio.hadoopConfig())
        .isNotNull()
        .containsEntry("fs.s3a.access.key", minio.accessKey())
        .containsEntry("fs.s3a.secret.key", minio.secretKey())
        .containsEntry("fs.s3a.endpoint", minio.s3endpoint());

    minio.s3put("some-key", RequestBody.fromString("hello world"));

    soft.assertThat(minio.s3BucketUri("some-key"))
        .isEqualTo(URI.create("s3://" + minio.bucket() + "/some-key"));

    try (S3Client client = minio.s3Client()) {
      soft.assertThat(client).isNotNull();

      try (ResponseInputStream<GetObjectResponse> getObject =
          client.getObject(b -> b.bucket(minio.bucket()).key("some-key"))) {
        GetObjectResponse getResponse = getObject.response();
        soft.assertThat(getResponse).isNotNull();
        soft.assertThat(getResponse.contentType()).startsWith("text/plain; charset");

        Path file = dir.resolve("file");
        Files.copy(getObject, file);
        soft.assertThat(file).content().isEqualTo("hello world");
      }
    }
  }
}
