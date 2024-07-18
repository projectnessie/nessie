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
package org.projectnessie.catalog.files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.files.api.BackendErrorCode;
import org.projectnessie.catalog.files.api.BackendErrorStatus;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.objectstoragemock.Bucket;
import org.projectnessie.objectstoragemock.MockObject;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractClients {
  @InjectSoftAssertions protected SoftAssertions soft;
  protected BackendExceptionMapper exceptionMapper;

  public static final String BUCKET_1 = "bucket1";
  public static final String BUCKET_2 = "bucket2";

  @BeforeEach
  protected void buildExceptionMapper() {
    exceptionMapper = addExceptionHandlers(BackendExceptionMapper.builder()).build();
  }

  @SuppressWarnings("resource")
  @Test
  public void writeReadDelete() throws Exception {
    try (ObjectStorageMock.MockServer server1 =
        ObjectStorageMock.builder()
            .putBuckets(BUCKET_1, newHeapStorageBucket().bucket())
            .build()
            .start()) {

      ObjectIO objectIO = buildObjectIO(server1, null);

      StorageUri uri = buildURI(BUCKET_1, "mykey");

      try (OutputStream output = objectIO.writeObject(uri)) {
        output.write("hello world".getBytes(UTF_8));
        // Explicitly close the output stream more than once
        output.close();
        //noinspection RedundantExplicitClose
        output.close();
      }
      String response1;
      try (InputStream input = objectIO.readObject(uri)) {
        response1 = new String(input.readAllBytes());
      }
      soft.assertThat(response1).isEqualTo("hello world");

      objectIO.deleteObjects(List.of(uri));
      // should not throw
      objectIO.deleteObjects(List.of(uri));

      soft.assertThatThrownBy(() -> objectIO.readObject(uri).readAllBytes())
          .extracting(e -> exceptionMapper.analyze(e))
          .extracting(Optional::get)
          .extracting(BackendErrorStatus::statusCode)
          .isEqualTo(BackendErrorCode.NOT_FOUND);
    }
  }

  @Test
  public void twoBucketsTwoServers() throws Exception {
    String answer1 = "hello world ";
    String answer2 = "hello other ";
    try (ObjectStorageMock.MockServer server1 =
            ObjectStorageMock.builder()
                .putBuckets(
                    BUCKET_1,
                    Bucket.builder()
                        .object(
                            key ->
                                MockObject.builder()
                                    .contentLength(answer1.length() + key.length())
                                    .writer(
                                        (range, output) ->
                                            output.write((answer1 + key).getBytes(UTF_8)))
                                    .contentType("text/plain")
                                    .build())
                        .build())
                .build()
                .start();
        ObjectStorageMock.MockServer server2 =
            ObjectStorageMock.builder()
                .putBuckets(
                    BUCKET_2,
                    Bucket.builder()
                        .object(
                            key ->
                                MockObject.builder()
                                    .contentLength(answer2.length() + key.length())
                                    .writer(
                                        (range, output) ->
                                            output.write((answer2 + key).getBytes(UTF_8)))
                                    .contentType("text/plain")
                                    .build())
                        .build())
                .build()
                .start()) {

      ObjectIO objectIO = buildObjectIO(server1, server2);

      String key1 = "meep";
      String key2 = "blah";
      String response1;
      String response2;
      try (InputStream input = objectIO.readObject(buildURI(BUCKET_1, key1))) {
        response1 = new String(input.readAllBytes());
      }
      try (InputStream input = objectIO.readObject(buildURI(BUCKET_2, key2))) {
        response2 = new String(input.readAllBytes());
      }
      soft.assertThat(response1).isEqualTo(answer1 + key1);
      soft.assertThat(response2).isEqualTo(answer2 + key2);
    }
  }

  protected abstract BackendExceptionMapper.Builder addExceptionHandlers(
      BackendExceptionMapper.Builder builder);

  protected abstract StorageUri buildURI(String bucket, String key);

  protected abstract ObjectIO buildObjectIO(
      ObjectStorageMock.MockServer server1, ObjectStorageMock.MockServer server2);
}
