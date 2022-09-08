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

  @FunctionalInterface
  public interface Deleter {
    boolean delete(S3ObjectIdentifier objectIdentifier);
  }

  @FunctionalInterface
  public interface Lister {
    Stream<ListElement> list(String prefix);
  }

  public interface ListElement {
    String key();

    MockObject object();
  }
}
