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
package com.dremio.nessie.versioned.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Consumer;

import org.immutables.value.Value;

import com.dremio.nessie.versioned.Hash;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Value.Immutable
abstract class Id implements InternalRef {

  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  public static final Id EMPTY = ImmutableId.builder().value(ByteString.copyFrom(new byte[20])).build();

  public static final int LENGTH = 20;

  abstract ByteString getValue();

  public static Id of(byte[] bytes) {
    return of(ByteString.copyFrom(bytes));
  }

  public static Id of(ByteBuffer bytes) {
    return of(ByteString.copyFrom(bytes));
  }

  public static Id of(ByteString bytes) {
    if (bytes.size() != LENGTH) {
      throw new IllegalArgumentException();
    }
    return ImmutableId.builder().value(bytes).build();
  }

  public static Id of(Hash hash) {
    ByteString bytes = hash.asBytes();
    Preconditions.checkArgument(bytes.size() == LENGTH, "Invalid key for this version store. Expected a binary value of "
        + "length %s but value was actually %s bytes long.", LENGTH, bytes.size());
    return Id.of(bytes);
  }

  public static Id build(ByteBuffer bytes) {
    return build(hasher -> {
      hasher.putBytes(bytes);
    });
  }

  public static Id build(String string) {
    return build(hasher -> {
      hasher.putString(string.toLowerCase(), StandardCharsets.UTF_8);
    });
  }

  public static Id build(ByteString bytes) {
    return build(hasher -> hashByteString(bytes, hasher));
  }

  public static Id build(Consumer<Hasher> consumer) {
    Hasher hasher = Hashing.sha256().newHasher();
    consumer.accept(hasher);
    byte[] outputBytes = hasher.hash().asBytes();
    return ImmutableId.builder().value(UnsafeByteOperations.unsafeWrap(outputBytes, 0, 20)).build();
  }

  @Override
  public String toString() {
    return toHash().asString();
  }

  public boolean isEmpty() {
    return this.equals(Id.EMPTY);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getValue());
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

  public static Id generateRandom() {
    byte[] bytes = new byte[LENGTH];
    RANDOM.get().nextBytes(bytes);
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

  public AttributeValue toAttributeValue() {
    return AttributeValue.builder().b(SdkBytes.fromByteArray(getValue().toByteArray())).build();
  }

  public void addToHash(Hasher hasher) {
    hashByteString(getValue(), hasher);
  }

  public Hash toHash() {
    return Hash.of(getValue());
  }

  public Map<String, AttributeValue> toKeyMap() {
    return ImmutableMap.of(DynamoStore.KEY_NAME, this.toAttributeValue());
  }

  public static Id fromAttributeValue(AttributeValue value) {
    return Id.of(ByteString.copyFrom(value.b().asByteBuffer()));
  }

  @Override
  public Type getType() {
    return Type.HASH;
  }

  @Override
  public Id getHash() {
    return this;
  }

  @Override
  public Id getId() {
    return this;
  }

}
