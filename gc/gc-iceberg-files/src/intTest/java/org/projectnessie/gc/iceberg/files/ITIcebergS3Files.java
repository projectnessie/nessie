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
package org.projectnessie.gc.iceberg.files;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.files.tests.AbstractFiles;
import org.projectnessie.minio.Minio;
import org.projectnessie.minio.MinioAccess;
import org.projectnessie.minio.MinioExtension;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.core.sync.RequestBody;

@ExtendWith(MinioExtension.class)
public class ITIcebergS3Files extends AbstractFiles {

  public static final String BUCKET_URI_PREFIX = "/path/";
  private MinioAccess minio;
  private IcebergFiles s3;
  private StorageUri baseUri;

  @BeforeEach
  void setUp(@Minio MinioAccess minio) {
    this.minio = minio;
    this.baseUri = StorageUri.of(minio.s3BucketUri(BUCKET_URI_PREFIX));
    Configuration config = new Configuration();
    minio.hadoopConfig().forEach(config::set);
    this.s3 =
        IcebergFiles.builder()
            .properties(minio.icebergProperties())
            .hadoopConfiguration(config)
            .build();
  }

  @AfterEach
  void tearDown() {
    if (s3 != null) {
      try {
        s3.close();
      } finally {
        s3 = null;
      }
    }
  }

  @Override
  protected StorageUri baseUri() {
    return baseUri;
  }

  @Override
  protected List<FileReference> prepareFiles(int numFiles) {
    ForkJoinPool forkJoinPool = new ForkJoinPool(4);
    try {
      return forkJoinPool.invoke(
          ForkJoinTask.adapt(
              () ->
                  IntStream.range(0, numFiles)
                      .mapToObj(AbstractFiles::dirAndFilename)
                      .parallel()
                      .peek(p -> minio.s3put(BUCKET_URI_PREFIX + p, RequestBody.empty()))
                      .map(p -> baseUri.resolve(p))
                      .map(p -> FileReference.of(baseUri.relativize(p), baseUri, -1L))
                      .collect(Collectors.toList())));
    } finally {
      forkJoinPool.shutdown();
    }
  }

  @Override
  protected FilesLister lister() {
    return s3;
  }

  @Override
  protected FileDeleter deleter() {
    return s3;
  }
}
