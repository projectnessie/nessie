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
package org.projectnessie.versioned.storage.common.persist;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;
import static org.projectnessie.versioned.storage.common.util.Hex.byteFromLong;
import static org.projectnessie.versioned.storage.common.util.Hex.hexChar;
import static org.projectnessie.versioned.storage.common.util.Hex.nibble;
import static org.projectnessie.versioned.storage.common.util.Hex.nibbleFromLong;
import static org.projectnessie.versioned.storage.common.util.Hex.stringToLong;
import static org.projectnessie.versioned.storage.common.util.Ser.putVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.readVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.varIntLen;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

public abstract class ObjId {
  // TODO Should this class actually be merged with the existing `Hash` class,
  //  need to move `Hash` to somewhere else though (project dependency issue ATM).

  public static final ObjId EMPTY_OBJ_ID = objIdHasher("empty").generate();

  public static ObjId zeroLengthObjId() {
    return ObjIdEmpty.INSTANCE;
  }

  public static ObjId objIdFromLongs(long l0, long l1, long l2, long l3) {
    return new ObjId256(l0, l1, l2, l3);
  }

  public static ObjId objIdFromByteAccessor(int size, ByteAccessor byteAt) {
    switch (size) {
      case 32:
        return ObjId256.fromByteAccessor(byteAt);
      case 0:
        return zeroLengthObjId();
      default:
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
          bytes[i] = byteAt.get(i);
        }
        return ObjIdGeneric.objIdFromByteArray(bytes);
    }
  }

  /**
   * Gets the bytes representation of the hash.
   *
   * @return the hash's bytes
   */
  public abstract ByteBuffer asByteBuffer();

  public abstract byte[] asByteArray();

  public abstract byte byteAt(int index);

  public abstract long longAt(int index);

  public ByteString asBytes() {
    return unsafeWrap(asByteArray());
  }

  public abstract ByteBuffer serializeTo(ByteBuffer target);

  /**
   * Retrieve the nibble (4 bit part) at the given index.
   *
   * <p>This retrieves the 4-bit value of the hex character from {@link #toString()} at the same
   * index.
   */
  public abstract int nibbleAt(int nibbleIndex);

  /** Return the size of the hash in bytes. */
  public abstract int size();

  /** Estimated object size on heap. */
  public abstract int heapSize();

  public abstract int serializedSize();

  /**
   * Creates a hash instance from its string representation.
   *
   * @param hash the string representation of the hash
   * @return a {@code Hash} instance
   * @throws IllegalArgumentException if {@code hash} is not a valid representation of a hash
   * @throws NullPointerException if {@code hash} is {@code null}
   */
  public static ObjId objIdFromString(@Nonnull String hash) {
    requireNonNull(hash, "hash string argument is null");
    int len = hash.length();
    checkArgument(len % 2 == 0, "hash length needs to be a multiple of two, was %s", len);

    switch (len >> 1) {
      case 0:
        return ObjIdEmpty.INSTANCE;
      case 32:
        return new ObjId256(hash);
      default:
        return new ObjIdGeneric(hash);
    }
  }

  /**
   * Creates an {@link ObjId} from its bytes representation, assuming that all data in {@code bytes}
   * belongs to the object id.
   *
   * @param bytes the serialized representation of the object id
   * @return a {@link ObjId} instance
   * @throws NullPointerException if {@code bytes} is {@code null}
   */
  public static ObjId objIdFromBytes(ByteString bytes) {
    int len = bytes.size();
    switch (len) {
      case 0:
        return ObjIdEmpty.INSTANCE;
      case 32:
        return new ObjId256(bytes);
      default:
        return new ObjIdGeneric(bytes);
    }
  }

  /**
   * Creates an {@link ObjId} from its bytes representation, assuming that all data in {@code bytes}
   * belongs to the object id.
   *
   * @param bytes the serialized representation of the object id
   * @return a {@link ObjId} instance
   * @throws NullPointerException if {@code bytes} is {@code null}
   */
  public static ObjId objIdFromByteArray(byte[] bytes) {
    return fromBytes(bytes.length, ByteBuffer.wrap(bytes));
  }

  /**
   * Creates an {@link ObjId} from its bytes representation, assuming that all (remaining) data in
   * {@code bytes} belongs to the object id.
   *
   * @param bytes the serialized representation of the object id
   * @return a {@link ObjId} instance
   * @throws NullPointerException if {@code bytes} is {@code null}
   */
  public static ObjId objIdFromByteBuffer(@Nonnull ByteBuffer bytes) {
    int len = bytes.remaining();
    return fromBytes(len, bytes);
  }

  public static ObjId randomObjId() {
    return ObjId256.random();
  }

  /**
   * Creates an {@link ObjId} instance from its bytes' representation, deserializing the var-int
   * encoded length from {@code bytes} first.
   *
   * @param bytes the serialized representation of the object id, represented by the var-int encoded
   *     length and the actual object id
   * @return a {@link ObjId} instance
   * @throws NullPointerException if {@code bytes} is {@code null}
   */
  public static ObjId deserializeObjId(@Nonnull ByteBuffer bytes) {
    int len = readVarInt(bytes);
    return fromBytes(len, bytes);
  }

  public static void skipObjId(@Nonnull ByteBuffer bytes) {
    int len = readVarInt(bytes);
    bytes.position(bytes.position() + len);
  }

  private static ObjId fromBytes(int len, ByteBuffer bytes) {
    switch (len) {
      case 0:
        return ObjIdEmpty.INSTANCE;
      case 32:
        return new ObjId256(bytes);
      default:
        ByteBuffer gen = bytes.duplicate();
        int lim = gen.position() + len;
        gen.limit(lim);
        bytes.position(lim);
        return new ObjIdGeneric(gen);
    }
  }

  /**
   * Generates a string representation of the hash suitable to be used with {@link
   * #objIdFromString(String)}.
   */
  @Override
  public String toString() {
    throw new UnsupportedOperationException("MUST BE IMPLEMENTED");
  }

  static final class ObjIdEmpty extends ObjId {
    private static final ByteBuffer BB_EMPTY = ByteBuffer.allocate(0);
    static final ObjId INSTANCE = new ObjIdEmpty();

    private ObjIdEmpty() {}

    @Override
    public String toString() {
      return "";
    }

    @Override
    public ByteBuffer asByteBuffer() {
      return BB_EMPTY;
    }

    @Override
    public byte[] asByteArray() {
      return new byte[0];
    }

    @Override
    public byte byteAt(int index) {
      throw new IllegalArgumentException("Invalid index " + index);
    }

    @Override
    public long longAt(int index) {
      throw new IllegalArgumentException("Invalid index " + index);
    }

    @Override
    public ByteBuffer serializeTo(ByteBuffer target) {
      return target.put((byte) 0);
    }

    @Override
    public int nibbleAt(int nibbleIndex) {
      throw new IllegalArgumentException("Invalid nibble index " + nibbleIndex);
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public int serializedSize() {
      return 1;
    }

    @Override
    public int heapSize() {
      /*
      org.projectnessie.versioned.storage.common.persist.ObjId$ObjIdEmpty object internals:
      OFF  SZ   TYPE DESCRIPTION               VALUE
        0   8        (object header: mark)     0x0000000000000001 (non-biasable; age: 0)
        8   4        (object header: class)    0x010cd918
       12   4        (object alignment gap)
      Instance size: 16 bytes
      Space losses: 0 bytes internal + 4 bytes external = 4 bytes total
      */
      return 16;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ObjIdEmpty;
    }
  }

  @VisibleForTesting
  static final class ObjId256 extends ObjId {
    private final long l0;
    private final long l1;
    private final long l2;
    private final long l3;

    private ObjId256(String hash) {
      l0 = stringToLong(hash, 0);
      l1 = stringToLong(hash, 16);
      l2 = stringToLong(hash, 32);
      l3 = stringToLong(hash, 48);
    }

    private ObjId256(ByteBuffer bytes) {
      l0 = bytes.getLong();
      l1 = bytes.getLong();
      l2 = bytes.getLong();
      l3 = bytes.getLong();
    }

    private ObjId256(long l0, long l1, long l2, long l3) {
      this.l0 = l0;
      this.l1 = l1;
      this.l2 = l2;
      this.l3 = l3;
    }

    private ObjId256(ByteString bytes) {
      this(bytes.asReadOnlyByteBuffer());
    }

    public static ObjId random() {
      ThreadLocalRandom tlr = ThreadLocalRandom.current();
      return new ObjId256(tlr.nextLong(), tlr.nextLong(), tlr.nextLong(), tlr.nextLong());
    }

    static ObjId fromByteAccessor(ByteAccessor byteAt) {
      long l0 = longFromByteAccessor(byteAt, 0);
      long l1 = longFromByteAccessor(byteAt, 8);
      long l2 = longFromByteAccessor(byteAt, 16);
      long l3 = longFromByteAccessor(byteAt, 24);
      return new ObjId256(l0, l1, l2, l3);
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
    public int size() {
      return 32;
    }

    @Override
    public int serializedSize() {
      return 33;
    }

    @Override
    public int heapSize() {
      /*
      org.projectnessie.versioned.storage.common.persist.ObjId$ObjId256 object internals:
      OFF  SZ   TYPE DESCRIPTION               VALUE
        0   8        (object header: mark)     0x0000000000000001 (non-biasable; age: 0)
        8   4        (object header: class)    0x010cb000
       12   4        (alignment/padding gap)
       16   8   long ObjId256.l0               0
       24   8   long ObjId256.l1               0
       32   8   long ObjId256.l2               0
       40   8   long ObjId256.l3               0
      Instance size: 48 bytes
      Space losses: 4 bytes internal + 0 bytes external = 4 bytes total
      */
      return 48;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(64);
      longToString(sb, l0);
      longToString(sb, l1);
      longToString(sb, l2);
      longToString(sb, l3);
      return sb.toString();
    }

    private static void longToString(StringBuilder sb, long v) {
      sb.append(hexChar((byte) (v >> 60)));
      sb.append(hexChar((byte) (v >> 56)));
      sb.append(hexChar((byte) (v >> 52)));
      sb.append(hexChar((byte) (v >> 48)));
      sb.append(hexChar((byte) (v >> 44)));
      sb.append(hexChar((byte) (v >> 40)));
      sb.append(hexChar((byte) (v >> 36)));
      sb.append(hexChar((byte) (v >> 32)));
      sb.append(hexChar((byte) (v >> 28)));
      sb.append(hexChar((byte) (v >> 24)));
      sb.append(hexChar((byte) (v >> 20)));
      sb.append(hexChar((byte) (v >> 16)));
      sb.append(hexChar((byte) (v >> 12)));
      sb.append(hexChar((byte) (v >> 8)));
      sb.append(hexChar((byte) (v >> 4)));
      sb.append(hexChar((byte) v));
    }

    @Override
    public ByteBuffer asByteBuffer() {
      return serializeToNoLen(ByteBuffer.allocate(32)).flip();
    }

    @Override
    public byte[] asByteArray() {
      byte[] r = new byte[32];
      serializeToNoLen(ByteBuffer.wrap(r));
      return r;
    }

    @Override
    public ByteBuffer serializeTo(ByteBuffer target) {
      return serializeToNoLen(target.put((byte) 32));
    }

    private ByteBuffer serializeToNoLen(ByteBuffer target) {
      return target.putLong(l0).putLong(l1).putLong(l2).putLong(l3);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ObjId256)) {
        return false;
      }
      ObjId256 objectId256 = (ObjId256) o;
      return l0 == objectId256.l0
          && l1 == objectId256.l1
          && l2 == objectId256.l2
          && l3 == objectId256.l3;
    }

    @Override
    public int hashCode() {
      return (int) (l0 >> 32L);
    }
  }

  @VisibleForTesting
  static final class ObjIdGeneric extends ObjId {
    private final byte[] bytes;

    private ObjIdGeneric(String hash) {
      int len = hash.length();
      byte[] bytes = new byte[len >> 1];
      for (int i = 0, c = 0; c < len; i++) {
        byte value = (byte) (nibble(hash.charAt(c++)) << 4);
        value |= nibble(hash.charAt(c++));
        bytes[i] = value;
      }
      checkArgument(bytes.length <= 256, "Hashes longer than 256 bytes are not supported");
      this.bytes = bytes;
    }

    private ObjIdGeneric(ByteBuffer bytes) {
      int length = bytes.remaining();
      this.bytes = new byte[length];
      bytes.duplicate().get(this.bytes, 0, length);
    }

    private ObjIdGeneric(ByteString bytes) {
      this(bytes.asReadOnlyByteBuffer());
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
    public byte byteAt(int index) {
      return bytes[index];
    }

    @Override
    public long longAt(int index) {
      index *= 8;
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
    public int size() {
      return bytes.length;
    }

    @Override
    public int serializedSize() {
      int sz = size();
      return sz + varIntLen(sz);
    }

    @Override
    public int heapSize() {
      /*
      org.projectnessie.versioned.storage.common.persist.ObjId$ObjIdGeneric object internals:
      OFF  SZ     TYPE DESCRIPTION               VALUE
        0   8          (object header: mark)     N/A
        8   4          (object header: class)    N/A
       12   4   byte[] ObjIdGeneric.bytes        N/A
      Instance size: 16 bytes
      Space losses: 0 bytes internal + 0 bytes external = 0 bytes total

      Array overhead: 16 bytes
      */
      return 16 + 16 + size();
    }

    @Override
    public String toString() {
      int len = bytes.length;
      StringBuilder sb = new StringBuilder(2 * len);
      for (int p = 0, i = 0; i < len; i++, p++) {
        byte b = bytes[p];
        sb.append(hexChar((byte) (b >> 4)));
        sb.append(hexChar(b));
      }
      return sb.toString();
    }

    @Override
    public ByteBuffer asByteBuffer() {
      return ByteBuffer.wrap(bytes);
    }

    @Override
    public byte[] asByteArray() {
      return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public ByteBuffer serializeTo(ByteBuffer target) {
      return putVarInt(target, bytes.length).put(bytes, 0, bytes.length);
    }

    @Override
    public int hashCode() {
      int r = bytes.length;

      int p = 0;
      int h = 0;
      if (r > 0) {
        h |= (bytes[p++] & 0xff) << 24;
        r--;
      }
      if (r > 0) {
        h |= ((bytes[p++] & 0xff) << 16);
        r--;
      }
      if (r > 0) {
        h |= ((bytes[p++] & 0xff) << 8);
        r--;
      }
      if (r > 0) {
        h |= (bytes[p] & 0xff);
      }
      return h;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ObjIdGeneric)) {
        return false;
      }
      ObjIdGeneric that = (ObjIdGeneric) obj;
      return Arrays.equals(this.bytes, that.bytes);
    }
  }

  @FunctionalInterface
  public interface ByteAccessor {
    byte get(int index);
  }
}
