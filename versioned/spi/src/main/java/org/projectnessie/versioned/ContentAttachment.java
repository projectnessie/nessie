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
package org.projectnessie.versioned;

import com.google.protobuf.ByteString;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Represents all fields of a "content attachment" in Nessie. */
@Value.Immutable
public interface ContentAttachment {

  ContentAttachmentKey getKey();

  /** Compression algorithm needed to uncompress {@link #getData()}. */
  Compression getCompression();

  /** Potentially compressed binary data. */
  ByteString getData();

  /**
   * Optimistic locking marker, filled and maintained by the "user" of this binary content object.
   */
  @Nullable
  String getVersion();

  static ImmutableContentAttachment.Builder builder() {
    return ImmutableContentAttachment.builder();
  }

  /**
   * Identifies the compression algorithm that was used to compress {@link
   * ContentAttachment#getData()}.
   */
  enum Compression {
    NONE,
    DEFLATE,
    GZIP
  }
}
