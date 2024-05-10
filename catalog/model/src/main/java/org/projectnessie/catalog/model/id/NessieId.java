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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Represents an ID in the Nessie Catalog. Users of this class must not interpret the value/content
 * of the ID. Nessie Catalog IDs consist only of bytes.
 */
@JsonSerialize(using = NessieIdSerializer.class)
@JsonDeserialize(using = NessieIdDeserializer.class)
public interface NessieId extends Hashable {

  static NessieId randomNessieId() {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    long l0 = rand.nextLong();
    long l1 = rand.nextLong();
    long l2 = rand.nextLong();
    long l3 = rand.nextLong();
    return NessieId256.nessieIdFromLongs(l0, l1, l2, l3);
  }

  static NessieId nessieIdFromBytes(byte[] bytes) {
    int l = bytes.length;
    if (l == 0) {
      return emptyNessieId();
    }
    if (l == 32) {
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      long l0 = buf.getLong();
      long l1 = buf.getLong();
      long l2 = buf.getLong();
      long l3 = buf.getLong();
      return nessieIdFromLongs(l0, l1, l2, l3);
    }
    return NessieIdGeneric.nessieIdFromBytes(Arrays.copyOf(bytes, bytes.length));
  }

  static NessieId emptyNessieId() {
    return NessieIdEmpty.INSTANCE;
  }

  static NessieId nessieIdFromByteAccessor(int size, ByteAccessor byteAt) {
    switch (size) {
      case 0:
        return emptyNessieId();
      case 32:
        return NessieId256.nessieIdFromByteAccessor(byteAt);
      default:
        return NessieIdGeneric.nessieIdFromByteAccessor(size, byteAt);
    }
  }

  static NessieId nessieIdFromLongs(long l0, long l1, long l2, long l3) {
    return NessieId256.nessieIdFromLongs(l0, l1, l2, l3);
  }

  static NessieId transientNessieId() {
    return NessieIdTransient.nessieIdTransient();
  }

  /** Size in bytes. */
  int size();

  byte byteAt(int index);

  long longAt(int index);

  ByteBuffer id();

  String idAsString();

  byte[] idAsBytes();

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(id());
  }

  @FunctionalInterface
  interface ByteAccessor {
    byte get(int index);
  }
}
