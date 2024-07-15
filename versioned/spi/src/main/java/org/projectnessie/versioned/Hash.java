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
package org.projectnessie.versioned;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations;

/**
 * Describes a specific point in time/history. Internally is a binary value but a string
 * representation is available.
 */
public abstract class Hash implements Ref {

  private static final char[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  /** Generates a string representation of the hash suitable to be used with {@link #of(String)}. */
  public abstract String asString();

  /**
   * Gets the bytes representation of the hash.
   *
   * @return the hash's bytes
   */
  public abstract ByteString asBytes();

  /**
   * Retrieve the nibble (4 bit part) at the given index.
   *
   * <p>This retrieves the 4-bit value of the hex character from {@link #asString()} at the same
   * index..
   */
  public abstract int nibbleAt(int nibbleIndex);

  /** Return the size of the hash in bytes. */
  public abstract int size();

  /**
   * Creates a hash instance from its string representation.
   *
   * @param hash the string representation of the hash
   * @return a {@code Hash} instance
   * @throws IllegalArgumentException if {@code hash} is not a valid representation of a hash
   * @throws NullPointerException if {@code hash} is {@code null}
   */
  public static Hash of(@Nonnull String hash) {
    requireNonNull(hash, "hash string argument is null");
    int len = hash.length();
    checkArgument(
        len % 2 == 0 && len > 0, "hash length needs to be a multiple of two, was %s", len);

    if (len >> 1 == 32) {
      return new Hash256(hash);
    }
    return new GenericHash(hash);
  }

  /**
   * Creates a hash instance from its bytes' representation.
   *
   * @param bytes the bytes' representation of the hash
   * @return a {@code Hash} instance
   * @throws NullPointerException if {@code hash} is {@code null}
   */
  public static Hash of(@Nonnull ByteString bytes) {
    if (bytes.size() == 32) {
      return new Hash256(bytes);
    }
    return new GenericHash(bytes);
  }

  @Override
  public String toString() {
    return "Hash " + asString();
  }

  @VisibleForTesting
  static byte nibble(char c) {
    if (c >= '0' && c <= '9') {
      return (byte) (c - '0');
    }
    if (c >= 'a' && c <= 'f') {
      return (byte) (c - 'a' + 10);
    }
    if (c >= 'A' && c <= 'F') {
      return (byte) (c - 'A' + 10);
    }
    throw new IllegalArgumentException("Illegal hex character '" + c + "'");
  }

  @VisibleForTesting
  static long bytesToLong(ByteString bytes, int off) {
    long l = (bytes.byteAt(off++) & 0xffL) << 56L;
    l |= (bytes.byteAt(off++) & 0xffL) << 48L;
    l |= (bytes.byteAt(off++) & 0xffL) << 40L;
    l |= (bytes.byteAt(off++) & 0xffL) << 32L;
    l |= (bytes.byteAt(off++) & 0xffL) << 24L;
    l |= (bytes.byteAt(off++) & 0xffL) << 16L;
    l |= (bytes.byteAt(off++) & 0xffL) << 8L;
    l |= bytes.byteAt(off) & 0xffL;
    return l;
  }

  @VisibleForTesting
  static void longToBytes(byte[] arr, int off, long v) {
    arr[off++] = (byte) (v >> 56L);
    arr[off++] = (byte) (v >> 48L);
    arr[off++] = (byte) (v >> 40L);
    arr[off++] = (byte) (v >> 32L);
    arr[off++] = (byte) (v >> 24L);
    arr[off++] = (byte) (v >> 16L);
    arr[off++] = (byte) (v >> 8L);
    arr[off] = (byte) v;
  }

  @VisibleForTesting
  static long stringToLong(String s, int off) {
    long l = ((long) nibble(s.charAt(off++))) << 60L;
    l |= ((long) nibble(s.charAt(off++))) << 56L;
    l |= ((long) nibble(s.charAt(off++))) << 52L;
    l |= ((long) nibble(s.charAt(off++))) << 48L;
    l |= ((long) nibble(s.charAt(off++))) << 44L;
    l |= ((long) nibble(s.charAt(off++))) << 40L;
    l |= ((long) nibble(s.charAt(off++))) << 36L;
    l |= ((long) nibble(s.charAt(off++))) << 32L;
    l |= ((long) nibble(s.charAt(off++))) << 28L;
    l |= ((long) nibble(s.charAt(off++))) << 24L;
    l |= ((long) nibble(s.charAt(off++))) << 20L;
    l |= ((long) nibble(s.charAt(off++))) << 16L;
    l |= ((long) nibble(s.charAt(off++))) << 12L;
    l |= ((long) nibble(s.charAt(off++))) << 8L;
    l |= ((long) nibble(s.charAt(off++))) << 4L;
    l |= nibble(s.charAt(off));
    return l;
  }

  @VisibleForTesting
  static int nibbleFromLong(long v, int index) {
    return (int) (v >> (60 - index * 4L)) & 0xf;
  }

  @VisibleForTesting
  static final class Hash256 extends Hash {
    private final long l0;
    private final long l1;
    private final long l2;
    private final long l3;

    private Hash256(String hash) {
      l0 = stringToLong(hash, 0);
      l1 = stringToLong(hash, 16);
      l2 = stringToLong(hash, 32);
      l3 = stringToLong(hash, 48);
    }

    private Hash256(ByteString bytes) {
      l0 = bytesToLong(bytes, 0);
      l1 = bytesToLong(bytes, 8);
      l2 = bytesToLong(bytes, 16);
      l3 = bytesToLong(bytes, 24);
    }

    @Override
    public int nibbleAt(int nibbleIndex) {
      if (nibbleIndex >= 0) {
        if (nibbleIndex < 16) {
          return nibbleFromLong(l0, nibbleIndex);
        }
        if (nibbleIndex < 32) {
          return nibbleFromLong(l1, nibbleIndex - 16);
        }
        if (nibbleIndex < 48) {
          return nibbleFromLong(l2, nibbleIndex - 32);
        }
        if (nibbleIndex < 64) {
          return nibbleFromLong(l3, nibbleIndex - 48);
        }
      }
      throw new IllegalArgumentException("Invalid nibble index " + nibbleIndex);
    }

    @Override
    public int size() {
      return 32;
    }

    @Override
    public String asString() {
      StringBuilder sb = new StringBuilder(64);
      longToString(sb, l0);
      longToString(sb, l1);
      longToString(sb, l2);
      longToString(sb, l3);
      return sb.toString();
    }

    private static void longToString(StringBuilder sb, long v) {
      sb.append(HEX[(int) (v >> 60) & 0xf]);
      sb.append(HEX[(int) (v >> 56) & 0xf]);
      sb.append(HEX[(int) (v >> 52) & 0xf]);
      sb.append(HEX[(int) (v >> 48) & 0xf]);
      sb.append(HEX[(int) (v >> 44) & 0xf]);
      sb.append(HEX[(int) (v >> 40) & 0xf]);
      sb.append(HEX[(int) (v >> 36) & 0xf]);
      sb.append(HEX[(int) (v >> 32) & 0xf]);
      sb.append(HEX[(int) (v >> 28) & 0xf]);
      sb.append(HEX[(int) (v >> 24) & 0xf]);
      sb.append(HEX[(int) (v >> 20) & 0xf]);
      sb.append(HEX[(int) (v >> 16) & 0xf]);
      sb.append(HEX[(int) (v >> 12) & 0xf]);
      sb.append(HEX[(int) (v >> 8) & 0xf]);
      sb.append(HEX[(int) (v >> 4) & 0xf]);
      sb.append(HEX[(int) v & 0xf]);
    }

    @Override
    public ByteString asBytes() {
      byte[] bytes = new byte[32];
      longToBytes(bytes, 0, l0);
      longToBytes(bytes, 8, l1);
      longToBytes(bytes, 16, l2);
      longToBytes(bytes, 24, l3);
      return UnsafeByteOperations.unsafeWrap(bytes);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Hash256)) {
        return false;
      }
      Hash256 hash256 = (Hash256) o;
      return l0 == hash256.l0 && l1 == hash256.l1 && l2 == hash256.l2 && l3 == hash256.l3;
    }

    @Override
    public int hashCode() {
      return (int) (l0 >> 32L);
    }
  }

  @VisibleForTesting
  static final class GenericHash extends Hash {
    private final byte[] bytes;

    private GenericHash(String hash) {
      int len = hash.length();
      byte[] bytes = new byte[len >> 1];
      for (int i = 0, c = 0; c < len; i++) {
        byte value = (byte) (nibble(hash.charAt(c++)) << 4);
        value |= nibble(hash.charAt(c++));
        bytes[i] = value;
      }
      this.bytes = bytes;
    }

    private GenericHash(ByteString bytes) {
      this.bytes = bytes.toByteArray();
    }

    @Override
    public int nibbleAt(int nibbleIndex) {
      byte b = bytes[nibbleIndex >> 1];
      if ((nibbleIndex & 1) == 0) {
        b >>= 4;
      }
      return b & 0xf;
    }

    @Override
    public int size() {
      return bytes.length;
    }

    @Override
    public String asString() {
      StringBuilder sb = new StringBuilder(2 * bytes.length);
      for (byte b : bytes) {
        sb.append(HEX[(b >> 4) & 0xf]);
        sb.append(HEX[b & 0xf]);
      }
      return sb.toString();
    }

    @Override
    public ByteString asBytes() {
      return UnsafeByteOperations.unsafeWrap(bytes);
    }

    @Override
    public int hashCode() {
      byte[] b = bytes;
      int h = (bytes[0] & 0xff) << 24;
      if (b.length > 1) {
        h |= ((bytes[1] & 0xff) << 16);
      }
      if (b.length > 2) {
        h |= ((bytes[2] & 0xff) << 8);
      }
      if (b.length > 3) {
        h |= (bytes[3] & 0xff);
      }
      return h;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof GenericHash)) {
        return false;
      }
      GenericHash that = (GenericHash) obj;
      return Arrays.equals(this.bytes, that.bytes);
    }
  }
}
