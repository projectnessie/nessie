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
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.minio.Minio;
import org.projectnessie.minio.MinioAccess;
import org.projectnessie.minio.MinioExtension;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

@ExtendWith(MinioExtension.class)
public class ITSparkIcebergNessieS3 extends AbstractITSparkIcebergNessieObjectStorage {

  public static final String S3_BUCKET_URI = "/my/prefix";
  public static final String S3_KEY_PREFIX = S3_BUCKET_URI.substring(1);

  @Minio static MinioAccess minio;

  @Override
  Storage storage() {
    return Storage.S3;
  }

  @Override
  protected String warehouseURI() {
    return minio.s3BucketUri(S3_BUCKET_URI).toString();
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    return minio.hadoopConfig();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    r.putAll(minio.icebergProperties());

    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.s3.endpoint", minio.s3endpoint());
    System.setProperty("aws.s3.accessKey", minio.accessKey());
    System.setProperty("aws.s3.secretAccessKey", minio.secretKey());

    return r;
  }

  @AfterEach
  void purgeS3() {
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(minio.bucket()).prefix(S3_KEY_PREFIX).build();
    minio.s3Client().listObjectsV2Paginator(request).stream()
        .map(ListObjectsV2Response::contents)
        .filter(contents -> !contents.isEmpty())
        .map(
            contents ->
                contents.stream()
                    .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                    .collect(Collectors.toList()))
        .forEach(
            keys ->
                minio
                    .s3Client()
                    .deleteObjects(
                        DeleteObjectsRequest.builder()
                            .bucket(minio.bucket())
                            .delete(Delete.builder().objects(keys).build())
                            .build()));
  }

  @Override
  IcebergFiles icebergFiles() {
    Configuration conf = new Configuration();
    minio.hadoopConfig().forEach(conf::set);
    return IcebergFiles.builder()
        .properties(minio.icebergProperties())
        .hadoopConfiguration(conf)
        .build();
  }

  @Override
  protected StorageUri bucketUri() {
    return StorageUri.of(minio.s3BucketUri(S3_BUCKET_URI));
  }
}
