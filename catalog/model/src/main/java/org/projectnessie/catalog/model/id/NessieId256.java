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
package org.projectnessie.catalog.model.id;

import java.nio.ByteBuffer;

final class NessieId256 implements NessieId {
  private static final char[] HEX =
      new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  private final long l0;
  private final long l1;
  private final long l2;
  private final long l3;

  private NessieId256(long l0, long l1, long l2, long l3) {
    this.l0 = l0;
    this.l1 = l1;
    this.l2 = l2;
    this.l3 = l3;
  }

  static NessieId nessieIdFromByteAccessor(ByteAccessor byteAt) {
    long l0 = longFromByteAccessor(byteAt, 0);
    long l1 = longFromByteAccessor(byteAt, 8);
    long l2 = longFromByteAccessor(byteAt, 16);
    long l3 = longFromByteAccessor(byteAt, 24);
    return nessieIdFromLongs(l0, l1, l2, l3);
  }

  private static long longFromByteAccessor(ByteAccessor byteAt, int offset) {
    long l = (((long) byteAt.get(offset++)) & 0xffL) << 56;
    l |= (((long) byteAt.get(offset++)) & 0xffL) << 48;
    l |= (((long) byteAt.get(offset++)) & 0xffL) << 40;
    l |= (((long) byteAt.get(offset++)) & 0xffL) << 32;
    l |= (((long) byteAt.get(offset++)) & 0xffL) << 24;
    l |= (((long) byteAt.get(offset++)) & 0xffL) << 16;
    l |= (((long) byteAt.get(offset++)) & 0xffL) << 8;
    l |= ((long) byteAt.get(offset)) & 0xffL;
    return l;
  }

  static NessieId nessieIdFromLongs(long l0, long l1, long l2, long l3) {
    return new NessieId256(l0, l1, l2, l3);
  }

  @Override
  public int size() {
    return 32;
  }

  @Override
  public byte byteAt(int index) {
    if (index >= 0) {
      if (index < 8) {
        return byteFromLong(l0, index);
      }
      if (index < 16) {
        return byteFromLong(l1, index - 8);
      }
      if (index < 24) {
        return byteFromLong(l2, index - 16);
      }
      if (index < 32) {
        return byteFromLong(l3, index - 24);
      }
    }
    throw new IllegalArgumentException("Invalid byte index " + index);
  }

  private static byte byteFromLong(long v, int index) {
    return (byte) (v >> (56 - index * 8L));
  }

  @Override
  public long longAt(int index) {
    switch (index) {
      case 0:
        return l0;
      case 1:
        return l1;
      case 2:
        return l2;
      case 3:
        return l3;
      default:
        throw new IllegalArgumentException("Invalid long index " + index);
    }
  }

  @Override
  public ByteBuffer id() {
    return ByteBuffer.wrap(idAsBytes());
  }

  @Override
  public String idAsString() {
    StringBuilder sb = new StringBuilder(64);
    idLongString(sb, l0);
    idLongString(sb, l1);
    idLongString(sb, l2);
    idLongString(sb, l3);
    return sb.toString();
  }

  private static void idLongString(StringBuilder sb, long v) {
    sb.append(HEX[(int) ((v >> 60) & 15)])
        .append(HEX[(int) ((v >> 56) & 15)])
        .append(HEX[(int) ((v >> 52) & 15)])
        .append(HEX[(int) ((v >> 48) & 15)])
        .append(HEX[(int) ((v >> 44) & 15)])
        .append(HEX[(int) ((v >> 40) & 15)])
        .append(HEX[(int) ((v >> 36) & 15)])
        .append(HEX[(int) ((v >> 32) & 15)])
        .append(HEX[(int) ((v >> 28) & 15)])
        .append(HEX[(int) ((v >> 24) & 15)])
        .append(HEX[(int) ((v >> 20) & 15)])
        .append(HEX[(int) ((v >> 16) & 15)])
        .append(HEX[(int) ((v >> 12) & 15)])
        .append(HEX[(int) ((v >> 8) & 15)])
        .append(HEX[(int) ((v >> 4) & 15)])
        .append(HEX[(int) (v & 15)]);
  }

  @Override
  public byte[] idAsBytes() {
    byte[] b = new byte[32];
    idLongBytes(l0, b, 0);
    idLongBytes(l1, b, 8);
    idLongBytes(l2, b, 16);
    idLongBytes(l3, b, 24);
    return b;
  }

  private static void idLongBytes(long v, byte[] b, int offset) {
    b[offset++] = (byte) ((v >> 56) & 0xff);
    b[offset++] = (byte) ((v >> 48) & 0xff);
    b[offset++] = (byte) ((v >> 40) & 0xff);
    b[offset++] = (byte) ((v >> 32) & 0xff);
    b[offset++] = (byte) ((v >> 24) & 0xff);
    b[offset++] = (byte) ((v >> 16) & 0xff);
    b[offset++] = (byte) ((v >> 8) & 0xff);
    b[offset] = (byte) (v & 0xff);
  }

  @Override
  public void hash(NessieIdHasher idHasher) {
    idHasher.hash(l0);
    idHasher.hash(l1);
    idHasher.hash(l2);
    idHasher.hash(l3);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof NessieId256) {
      NessieId256 other = (NessieId256) obj;
      return other.l0 == l0 && other.l1 == l1 && other.l2 == l2 && other.l3 == l3;
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
  public int hashCode() {
    return (int) (l0 >> 32L);
  }

  @Override
  public String toString() {
    return idAsString();
  }
}
