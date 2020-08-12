/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.versioned;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Describes a specific point in time/history. Internally is a binary value but a string representation is available.
 */
public final class Hash implements Ref {

  private static final BaseEncoding ENCODING = BaseEncoding.base16().lowerCase();

  private final ByteString bytes;

  private Hash(ByteString bytes) {
    this.bytes = bytes;
  }

  public String asString() {
    return ENCODING.encode(bytes.toByteArray());
  }

  public static Hash of(String hash) {
    byte[] bytes = ENCODING.decode(hash);
    return new Hash(UnsafeByteOperations.unsafeWrap(bytes));
  }

  @Override
  public final String toString() {
    return "Hash " + asString();
  }
}
