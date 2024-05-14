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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.util.Hex.hexChar;
import static org.projectnessie.versioned.storage.common.util.Hex.nibble;

import jakarta.annotation.Nonnull;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

@Value.Immutable
public interface PagingToken {
  @Value.Parameter(order = 1)
  ByteString token();

  @Value.NonAttribute
  default boolean isEmpty() {
    return token().isEmpty();
  }

  @Nonnull
  default String asString() {
    ByteString t = token();
    int size = t.size();
    StringBuilder sb = new StringBuilder(size * 2);
    for (int i = 0; i < size; i++) {
      byte b = t.byteAt(i);
      sb.append(hexChar((byte) (b >> 4)));
      sb.append(hexChar(b));
    }
    return sb.toString();
  }

  @Nonnull
  static PagingToken fromString(@Nonnull String tokenAsString) {
    int len = tokenAsString.length();
    if ((len & 1) == 1) {
      throw new IllegalArgumentException("Invalid token string representation");
    }
    int bytes = len >> 1;
    byte[] arr = new byte[bytes];
    for (int i = 0, c = 0; i < bytes; i++) {
      arr[i] =
          (byte) ((nibble(tokenAsString.charAt(c++)) << 4) | nibble(tokenAsString.charAt(c++)));
    }
    return pagingToken(unsafeWrap(arr));
  }

  @Nonnull
  static PagingToken pagingToken(@Nonnull ByteString token) {
    return ImmutablePagingToken.of(token);
  }

  static PagingToken emptyPagingToken() {
    return pagingToken(ByteString.EMPTY);
  }
}
