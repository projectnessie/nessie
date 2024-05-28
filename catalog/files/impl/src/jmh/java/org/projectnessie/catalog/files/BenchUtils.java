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

import java.io.InputStream;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.projectnessie.objectstoragemock.Bucket;
import org.projectnessie.objectstoragemock.ImmutableObjectStorageMock;
import org.projectnessie.objectstoragemock.MockObject;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

public final class BenchUtils {
  private BenchUtils() {}

  public static ObjectStorageMock.MockServer mockServer(
      Consumer<ImmutableObjectStorageMock.Builder> mockCustomizer) {
    byte[] zeroBytes = new byte[4096];
    ImmutableObjectStorageMock.Builder mock =
        ObjectStorageMock.builder()
            .putBuckets(
                "bucket",
                Bucket.builder()
                    .lister((prefix, offset) -> Stream.empty())
                    .object(
                        key -> {
                          int size = key.startsWith("s-") ? Integer.parseInt(key.substring(2)) : 0;
                          MockObject.Writer writer =
                              (range, out) -> {
                                for (int remain = size; remain > 0; ) {
                                  int sz = Math.min(remain, zeroBytes.length);
                                  out.write(zeroBytes, 0, sz);
                                  remain -= sz;
                                }
                              };
                          return MockObject.builder().contentLength(size).writer(writer).build();
                        })
                    .updater(
                        (key, mode) ->
                            new Bucket.ObjectUpdater() {
                              @Override
                              public MockObject commit() {
                                return MockObject.builder().build();
                              }

                              @Override
                              public Bucket.ObjectUpdater setContentType(String contentType) {
                                return this;
                              }

                              @Override
                              public Bucket.ObjectUpdater flush() {
                                return this;
                              }

                              @Override
                              public Bucket.ObjectUpdater append(long position, InputStream data) {
                                return this;
                              }
                            })
                    .build());
    mockCustomizer.accept(mock);
    return mock.build().start();
  }
}
