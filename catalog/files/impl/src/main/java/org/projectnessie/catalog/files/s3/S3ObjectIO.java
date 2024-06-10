/*
 * Copyright (C) 2023 Dremio
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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.catalog.files.s3.S3Utils.isS3scheme;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectIO implements ObjectIO {

  private final S3ClientSupplier s3clientSupplier;

  public S3ObjectIO(S3ClientSupplier s3clientSupplier) {
    this.s3clientSupplier = s3clientSupplier;
  }

  @Override
  public void ping(StorageUri uri) throws IOException {
    S3Client s3client = s3clientSupplier.getClient(uri);
    try {
      s3client.headBucket(b -> b.bucket(uri.requiredAuthority()));
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    checkArgument(uri != null, "Invalid location: null");
    String scheme = uri.scheme();
    checkArgument(isS3scheme(scheme), "Invalid S3 scheme: %s", uri);

    S3Client s3client = s3clientSupplier.getClient(uri);

    try {
      return s3client.getObject(
          GetObjectRequest.builder()
              .bucket(uri.requiredAuthority())
              .key(withoutLeadingSlash(uri))
              .build());
    } catch (SdkServiceException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputStream writeObject(StorageUri uri) {
    checkArgument(uri != null, "Invalid location: null");
    checkArgument(isS3scheme(uri.scheme()), "Invalid S3 scheme: %s", uri);

    return new ByteArrayOutputStream() {
      @Override
      public void close() throws IOException {
        super.close();

        S3Client s3client = s3clientSupplier.getClient(uri);

        s3client.putObject(
            PutObjectRequest.builder()
                .bucket(uri.requiredAuthority())
                .key(withoutLeadingSlash(uri))
                .build(),
            RequestBody.fromBytes(toByteArray()));
      }
    };
  }

  private static String withoutLeadingSlash(StorageUri uri) {
    String path = uri.requiredPath();
    return path.startsWith("/") ? path.substring(1) : path;
  }
}
