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
package com.dremio.nessie.versioned.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

public final class Id {

  public static final int LENGTH = 20;
  public static final Id EMPTY = new Id(ByteString.copyFrom(new byte[LENGTH]));

  private final ByteString value;

  private Id(ByteString value) {
    Preconditions.checkNotNull(value);
    Preconditions.checkArgument(value.size() == LENGTH, "Invalid key for this version store. Expected a binary value of "
        + "length %s but value was actually %s bytes long.", LENGTH, value.size());
    this.value = value;
  }

  public ByteString getValue() {
    return value;
  }

  public static Id of(byte[] bytes) {
    return of(ByteString.copyFrom(bytes));
  }

  public static Id of(ByteBuffer bytes) {
    return of(ByteString.copyFrom(bytes));
  }

  public static Id of(ByteString bytes) {
    return new Id(bytes);
  }

  /**
   * Create an Id based on a VersionStore Hash.
   * @param hash The hash the id should use.
   * @return The Id object
   * @throws ReferenceNotFoundException Thrown if the hash can't be a valid Id.
   */
  public static Id of(Hash hash) throws ReferenceNotFoundException {
    ByteString bytes = hash.asBytes();
    if (bytes.size() != LENGTH) {
      throw ReferenceNotFoundException.forReference(hash);
    }
    return Id.of(bytes);
  }

  /**
   * Create an Id based on a collection of arbitrary bytes.
   * @param bytes The bytes to hash.
   * @return The generated id.
   */
  public static Id build(ByteBuffer bytes) {
    return build(hasher -> {
      hasher.putBytes(bytes);
    });
  }

  /**
   * Create a Id by hashing the lower case value of the provided string.
   * @param string The string to hash
   * @return The generated Id object.
   */
  public static Id build(String string) {
    return build(hasher -> {
      hasher.putString(string.toLowerCase(Locale.US), StandardCharsets.UTF_8);
    });
  }

  public static Id build(ByteString bytes) {
    return build(hasher -> hashByteString(bytes, hasher));
  }

  /**
   * Create an id by using a provided hasher.
   * @param consumer The lambda that contains the require hashing.
   * @return The generated Id.
   */
  public static Id build(Consumer<Hasher> consumer) {
    Hasher hasher = Hashing.sha256().newHasher();
    consumer.accept(hasher);
    byte[] outputBytes = hasher.hash().asBytes();
    return new Id(UnsafeByteOperations.unsafeWrap(outputBytes, 0, 20));
  }

  @Override
  public String toString() {
    return toHash().asString();
  }

  public boolean isEmpty() {
    return this.equals(Id.EMPTY);
  }

  public boolean isNonEmpty() {
    return !isEmpty();
  }

  @Override
  public int hashCode() {
    return getValue().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Id)) {
      return false;
    }
    Id other = (Id) obj;
    return Objects.equals(getValue(), other.getValue());
  }

  /**
   * Generate a random valid Id.
   * @return The generated Id.
   */
  public static Id generateRandom() {
    byte[] bytes = new byte[LENGTH];
    ThreadLocalRandom.current().nextBytes(bytes);
    return Id.of(bytes);
  }

  public byte[] toBytes() {
    return getValue().toByteArray();
  }

  private static void hashByteString(ByteString bytes, Hasher hasher) {
    try {
      UnsafeByteOperations.unsafeWriteTo(bytes, new ByteOutput() {

        @Override
        public void write(byte value) throws IOException {
          hasher.putByte(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
          hasher.putBytes(value, offset, length);
        }

        @Override
        public void write(ByteBuffer value) throws IOException {
          hasher.putBytes(value);
        }

        @Override
        public void writeLazy(byte[] value, int offset, int length) throws IOException {
          write(value, offset, length);
        }

        @Override
        public void writeLazy(ByteBuffer value) throws IOException {
          write(value);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e); // can't happen.
    }
  }

  public Entity toEntity() {
    return Entity.ofBinary(getValue());
  }

  public Hash toHash() {
    return Hash.of(getValue());
  }

  public static Id fromEntity(Entity value) {
    return Id.of(value.getBinary());
  }

  public Id getHash() {
    return this;
  }

}
