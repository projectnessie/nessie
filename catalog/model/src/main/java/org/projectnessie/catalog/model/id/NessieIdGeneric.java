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
package org.projectnessie.catalog.model.id;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents an ID in the Nessie Catalog. Users of this class must not interpret the value/content
 * of the ID. Nessie Catalog IDs consist only of bytes.
 */
public final class NessieIdGeneric implements NessieId {
  private static final char[] HEX =
      new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  private final byte[] id;

  private NessieIdGeneric(byte[] id) {
    this.id = id;
  }

  static NessieIdGeneric nessieIdFromBytes(byte[] bytes) {
    return new NessieIdGeneric(bytes);
  }

  static NessieId nessieIdFromByteAccessor(int size, ByteAccessor byteAt) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = byteAt.get(i);
    }
    return new NessieIdGeneric(bytes);
  }

  @Override
  public int size() {
    return id.length;
  }

  @Override
  public byte byteAt(int index) {
    return id[index];
  }

  @Override
  public long longAt(int index) {
    index *= 4;
    long l = 0;
    l |= (((long) byteAt(index++)) & 0xffL) << 56;
    l |= (((long) byteAt(index++)) & 0xffL) << 48;
    l |= (((long) byteAt(index++)) & 0xffL) << 40;
    l |= (((long) byteAt(index++)) & 0xffL) << 32;
    l |= (((long) byteAt(index++)) & 0xffL) << 24;
    l |= (((long) byteAt(index++)) & 0xffL) << 16;
    l |= (((long) byteAt(index++)) & 0xffL) << 8;
    l |= ((long) byteAt(index)) & 0xffL;
    return l;
  }

  @Override
  public ByteBuffer id() {
    return ByteBuffer.wrap(id).asReadOnlyBuffer();
  }

  @Override
  public String idAsString() {
    return idBytesString(new StringBuilder(id.length * 2)).toString();
  }

  private StringBuilder idBytesString(StringBuilder sb) {
    for (byte b : id) {
      sb.append(HEX[(b >> 4) & 15]);
      sb.append(HEX[b & 15]);
    }
    return sb;
  }

  @Override
  public byte[] idAsBytes() {
    return id;
  }

  @Override
  public int hashCode() {
    int r = id.length;

    int p = 0;
    int h = 0;
    if (r > 0) {
      h |= (id[p++] & 0xff) << 24;
      r--;
    }
    if (r > 0) {
      h |= ((id[p++] & 0xff) << 16);
      r--;
    }
    if (r > 0) {
      h |= ((id[p++] & 0xff) << 8);
      r--;
    }
    if (r > 0) {
      h |= (id[p] & 0xff);
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof NessieIdGeneric) {
      NessieIdGeneric other = (NessieIdGeneric) obj;
      return Arrays.equals(id, other.id);
    }
    if (obj instanceof NessieId) {
      NessieId other = (NessieId) obj;
      int size = size();
      if (other.size() != size) {
        return false;
      }
      for (int i = 0; i < size; i++) {
        if (other.byteAt(i) != byteAt(i)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return idAsString();
  }
}
