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
package org.projectnessie.objectstoragemock;

import static java.lang.System.currentTimeMillis;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Bucket {

  public static ImmutableBucket.Builder builder() {
    return ImmutableBucket.builder();
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
    return (String prefix, String offset) -> Stream.empty();
  }

  @Value.Default
  public Updater updater() {
    return (objectName, mode) -> {
      throw new UnsupportedOperationException();
    };
  }

  @FunctionalInterface
  public interface Deleter {
    boolean delete(String key);
  }

  @FunctionalInterface
  public interface Updater {
    ObjectUpdater update(String key, UpdaterMode mode);
  }

  public enum UpdaterMode {
    CREATE_NEW,
    UPDATE,
  }

  public interface ObjectUpdater {
    @CanIgnoreReturnValue
    ObjectUpdater append(long position, InputStream data);

    @CanIgnoreReturnValue
    ObjectUpdater flush();

    @CanIgnoreReturnValue
    ObjectUpdater setContentType(String contentType);

    @CanIgnoreReturnValue
    MockObject commit();
  }

  @FunctionalInterface
  public interface Lister {
    Stream<ListElement> list(String prefix, String offset);
  }

  public interface ListElement {
    String key();

    MockObject object();
  }

  public static Bucket createHeapStorageBucket() {
    TreeMap<String, MockObject> objects = new TreeMap<>();

    return Bucket.builder()
        .object(
            key -> {
              synchronized (objects) {
                return objects.get(key);
              }
            })
        .updater(
            (key, mode) -> {
              MockObject expected;
              synchronized (objects) {
                expected = objects.get(key);
                switch (mode) {
                  case CREATE_NEW:
                    if (expected != null) {
                      throw new IllegalStateException("Object '" + key + "' already exists");
                    }
                    break;
                  case UPDATE:
                    if (expected == null) {
                      throw new IllegalStateException("Object '" + key + "' does not exist");
                    }
                    break;
                  default:
                    throw new IllegalArgumentException(mode.name());
                }
              }
              return new HeapObjectUpdater(expected, objects, key);
            })
        .lister(
            (prefix, offset) -> {
              Collection<String> keys;
              synchronized (objects) {
                keys = new ArrayList<>();
                if (prefix != null) {
                  String startAt = offset != null && offset.compareTo(prefix) > 0 ? offset : prefix;
                  for (String key : objects.tailMap(startAt, true).keySet()) {
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
                return objects.remove(oid) != null;
              }
            })
        .build();
  }

  private static class HeapObjectUpdater implements ObjectUpdater {
    private final MockObject expected;
    private final TreeMap<String, MockObject> objects;
    private final String key;
    private final ImmutableMockObject.Builder object;

    public HeapObjectUpdater(MockObject expected, TreeMap<String, MockObject> objects, String key) {
      this.expected = expected;
      this.objects = objects;
      this.key = key;
      this.object = ImmutableMockObject.builder();
      if (expected != null) {
        this.object.from(expected);
      }
    }

    @Override
    public ObjectUpdater setContentType(String contentType) {
      object.contentType(contentType);
      return this;
    }

    @Override
    public ObjectUpdater flush() {
      return this;
    }

    @Override
    public MockObject commit() {
      synchronized (objects) {
        MockObject curr = objects.get(key);
        if (curr != expected) {
          throw new ConcurrentModificationException(
              "Object '" + key + "' has been modified concurrently.");
        }
        MockObject obj = object.lastModified(currentTimeMillis()).build();
        objects.put(key, obj);
        return obj;
      }
    }

    @Override
    public ObjectUpdater append(long position, InputStream data) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        object.build().writer().write(null, out);
        data.transferTo(out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      byte[] newData = out.toByteArray();

      object
          .contentLength(newData.length)
          // .etag("etag")
          // .storageClass(StorageClass.STANDARD)
          .writer(
              ((range, output) -> {
                if (range == null) {
                  output.write(newData);
                } else {
                  output.write(newData, (int) range.start(), (int) (range.end() - range.start()));
                }
              }));

      return this;
    }
  }
}
