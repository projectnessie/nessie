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
package org.projectnessie.testing.floci.gcp;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.net.URI;
import java.nio.ByteBuffer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({FlociGcpExtension.class, SoftAssertionsExtension.class})
public class ITFlociGcpExtension {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void smokeTest(
      @FlociGcp(bucket = "mybucket", oauth2token = "myoauth", projectId = "myproject")
          FlociGcpAccess flociGcp)
      throws Exception {
    soft.assertThat(flociGcp.localAddress()).isNotEmpty();
    soft.assertThat(flociGcp.baseUri()).isNotEmpty().startsWith("http");
    soft.assertThat(flociGcp.bucketUri()).extracting(URI::getScheme).isEqualTo("gs");

    soft.assertThat(flociGcp.bucket()).isNotEmpty().isEqualTo("mybucket");
    soft.assertThat(flociGcp.oauth2token()).isNotEmpty().isEqualTo("myoauth");
    soft.assertThat(flociGcp.projectId()).isNotEmpty().isEqualTo("myproject");

    soft.assertThat(flociGcp.icebergProperties())
        .containsEntry("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
        .containsEntry("gcs.project-id", flociGcp.projectId())
        .containsEntry("gcs.service.host", flociGcp.baseUri())
        .containsEntry("gcs.oauth2.token", flociGcp.oauth2token());

    soft.assertThat(flociGcp.hadoopConfig())
        .isNotNull()
        .containsEntry("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .containsEntry(
            "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .containsEntry("fs.gs.storage.root.url", flociGcp.baseUri())
        .containsEntry("fs.gs.project.id", flociGcp.projectId());

    try (Storage storage = flociGcp.newStorage()) {
      BlobId blobId = BlobId.of(flociGcp.bucket(), "some-key");
      byte[] data = "hello world".getBytes(UTF_8);

      soft.assertThat(flociGcp.bucketUri(blobId.getName()))
          .isEqualTo(URI.create("gs://" + flociGcp.bucket() + "/" + blobId.getName()));

      storage.create(BlobInfo.newBuilder(blobId).setContentType("text/plain").build(), data);
      soft.assertThat(storage.readAllBytes(blobId)).isEqualTo(data);
      soft.assertThat(storage.get(blobId)).extracting(Blob::getContentType).isEqualTo("text/plain");

      blobId = BlobId.of(flociGcp.bucket(), "other-key");
      try (WriteChannel channel =
          storage.writer(BlobInfo.newBuilder(blobId).setContentType("text/plain").build())) {
        channel.write(ByteBuffer.wrap(data));
      }
      soft.assertThat(storage.readAllBytes(blobId)).isEqualTo(data);
      soft.assertThat(storage.get(blobId)).extracting(Blob::getContentType).isEqualTo("text/plain");
    }
  }
}
