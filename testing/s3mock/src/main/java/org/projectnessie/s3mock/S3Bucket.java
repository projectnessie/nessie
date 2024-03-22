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
package org.projectnessie.s3mock;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.s3mock.data.S3ObjectIdentifier;

@Value.Immutable
public abstract class S3Bucket {

  public static ImmutableS3Bucket.Builder builder() {
    return ImmutableS3Bucket.builder();
  }

  @Value.Default
  public String creationDate() {
    return Instant.now().toString();
  }

  @Value.Default
  public ObjectRetriever object() {
    return x -> null;
  }

  @Value.Default
  public Deleter deleter() {
    return x -> false;
  }

  @Value.Default
  public Lister lister() {
    return (String prefix) -> Stream.empty();
  }

  @Value.Default
  public Storer storer() {
    return (objectName, contentType, data) -> {
      throw new UnsupportedOperationException();
    };
  }

  @FunctionalInterface
  public interface Deleter {
    boolean delete(S3ObjectIdentifier objectIdentifier);
  }

  @FunctionalInterface
  public interface Storer {
    void store(String key, String contentType, byte[] data);
  }

  @FunctionalInterface
  public interface Lister {
    Stream<ListElement> list(String prefix);
  }

  public interface ListElement {
    String key();

    MockObject object();
  }

  public static S3Bucket createHeapStorageBucket() {
    TreeMap<String, MockObject> objects = new TreeMap<>();

    return S3Bucket.builder()
        .object(
            key -> {
              synchronized (objects) {
                return objects.get(key);
              }
            })
        .storer(
            (key, contentType, data) -> {
              synchronized (objects) {
                objects.putIfAbsent(
                    key,
                    MockObject.builder()
                        .contentLength(data.length)
                        // .etag("etag")
                        // .storageClass(StorageClass.STANDARD)
                        .lastModified(System.currentTimeMillis())
                        .writer(
                            ((range, output) -> {
                              if (range == null) {
                                output.write(data);
                              } else {
                                output.write(
                                    data, (int) range.start(), (int) (range.end() - range.start()));
                              }
                            }))
                        .build());
              }
            })
        .lister(
            prefix -> {
              Collection<String> keys;
              synchronized (objects) {
                keys = new ArrayList<>();
                if (prefix != null) {
                  for (String key : objects.tailMap(prefix, true).keySet()) {
                    if (!key.startsWith(prefix)) {
                      break;
                    }
                    keys.add(key);
                  }
                } else {
                  keys.addAll(objects.keySet());
                }
              }
              return keys.stream()
                  .map(
                      key ->
                          new ListElement() {
                            @Override
                            public String key() {
                              return key;
                            }

                            @Override
                            public MockObject object() {
                              synchronized (objects) {
                                return objects.get(key);
                              }
                            }
                          });
            })
        .deleter(
            oid -> {
              synchronized (objects) {
                return objects.remove(oid.key()) != null;
              }
            })
        .build();
  }
}
