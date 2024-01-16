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
package org.projectnessie.versioned.storage.common.persist;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.Entry.comparingByKey;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("UnstableApiUsage")
final class ObjIdHasherImpl implements ObjIdHasher {
  private final Hasher hasher;

  ObjIdHasherImpl(Hasher hasher) {
    this.hasher = hasher;
  }

  ObjIdHasherImpl() {
    this(Hashing.sha256().newHasher());
  }

  @Override
  public ObjIdHasher hash(boolean value) {
    hasher.putBoolean(value);
    return this;
  }

  @Override
  public ObjIdHasher hash(char value) {
    hasher.putChar(value);
    return this;
  }

  @Override
  public ObjIdHasher hash(int value) {
    hasher.putInt(value);
    return this;
  }

  @Override
  public ObjIdHasher hash(long value) {
    hasher.putLong(value);
    return this;
  }

  @Override
  public ObjIdHasher hash(float value) {
    hasher.putFloat(value);
    return this;
  }

  @Override
  public ObjIdHasher hash(double value) {
    hasher.putDouble(value);
    return this;
  }

  @Override
  public ObjIdHasher hash(Enum<?> value) {
    if (value != null) {
      hash(value.name());
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(Boolean value) {
    if (value != null) {
      hasher.putBoolean(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(Integer value) {
    if (value != null) {
      hasher.putInt(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(Long value) {
    if (value != null) {
      hasher.putLong(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(Float value) {
    if (value != null) {
      hasher.putFloat(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(Double value) {
    if (value != null) {
      hasher.putDouble(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(String value) {
    if (value != null) {
      hasher.putString(value, UTF_8);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(byte[] value) {
    if (value != null) {
      hasher.putBytes(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(ByteBuffer value) {
    if (value != null) {
      hasher.putBytes(value);
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(UUID value) {
    if (value != null) {
      hasher.putLong(value.getMostSignificantBits());
      hasher.putLong(value.getLeastSignificantBits());
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(ObjId value) {
    if (value != null) {
      hasher.putBytes(value.asByteBuffer());
    }
    return this;
  }

  @Override
  public ObjIdHasher hash(Hashable value) {
    if (value != null) {
      value.hash(this);
    }
    return this;
  }

  @Override
  public ObjIdHasher hashCollection(Collection<? extends Hashable> value) {
    if (value != null) {
      value.forEach(this::hash);
    }
    return this;
  }

  @Override
  public ObjIdHasher hashUuidCollection(Collection<UUID> value) {
    if (value != null) {
      value.forEach(
          uuid -> hash(uuid.getMostSignificantBits()).hash(uuid.getLeastSignificantBits()));
    }
    return this;
  }

  @Override
  public ObjIdHasher hashIntCollection(Collection<Integer> value) {
    if (value != null) {
      value.forEach(this::hash);
    }
    return this;
  }

  @Override
  public ObjIdHasher hashLongCollection(Collection<Long> value) {
    if (value != null) {
      value.forEach(this::hash);
    }
    return this;
  }

  @Override
  public ObjIdHasher hashStringToStringMap(Map<String, String> map) {
    if (map != null) {
      map.entrySet().stream()
          .sorted(comparingByKey())
          .forEach(e -> hash(e.getKey()).hash(e.getValue()));
    }
    return this;
  }

  @Override
  public ObjId generate() {
    return ObjId.objIdFromByteArray(hasher.hash().asBytes());
  }
}
