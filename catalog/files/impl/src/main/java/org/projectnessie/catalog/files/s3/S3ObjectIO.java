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
import static java.time.temporal.ChronoUnit.SECONDS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import org.projectnessie.catalog.files.api.BackendThrottledException;
import org.projectnessie.catalog.files.api.NonRetryableException;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectIO implements ObjectIO {

  private final S3ClientSupplier s3clientSupplier;
  private final Clock clock;

  public S3ObjectIO(S3ClientSupplier s3clientSupplier, Clock clock) {
    this.s3clientSupplier = s3clientSupplier;
    this.clock = clock;
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    checkArgument(uri != null, "Invalid location: null");
    checkArgument("s3".equals(uri.scheme()), "Invalid S3 scheme: %s", uri);

    S3Client s3client = s3clientSupplier.getClient(uri);

    try {
      return s3client.getObject(
          GetObjectRequest.builder()
              .bucket(uri.requiredAuthority())
              .key(uri.requiredPath())
              .build());
    } catch (SdkServiceException e) {
      if (e.isThrottlingException()) {
        throw new BackendThrottledException(
            clock
                .instant()
                .plus(s3clientSupplier.s3config().retryAfter().orElse(Duration.of(10, SECONDS))),
            "S3 throttled",
            e);
      }
      throw new NonRetryableException(e);
    }
  }

  @Override
  public OutputStream writeObject(StorageUri uri) {
    checkArgument(uri != null, "Invalid location: null");
    checkArgument("s3".equals(uri.scheme()), "Invalid S3 scheme: %s", uri);

    return new ByteArrayOutputStream() {
      @Override
      public void close() throws IOException {
        super.close();

        S3Client s3client = s3clientSupplier.getClient(uri);

        s3client.putObject(
            PutObjectRequest.builder()
                .bucket(uri.requiredAuthority())
                .key(uri.requiredPath())
                .build(),
            RequestBody.fromBytes(toByteArray()));
      }
    };
  }

  @Override
  public boolean isValidUri(StorageUri uri) {
    return uri != null && "s3".equals(uri.scheme());
  }
}
