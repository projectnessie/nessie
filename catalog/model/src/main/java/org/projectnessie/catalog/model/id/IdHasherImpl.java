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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.Entry.comparingByKey;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

@SuppressWarnings("UnstableApiUsage")
final class IdHasherImpl implements NessieIdHasher {
  private final Hasher hashing;

  IdHasherImpl() {
    hashing = Hashing.sha256().newHasher();
  }

  @Override
  public NessieIdHasher hash(boolean value) {
    hashing.putBoolean(value);
    return this;
  }

  @Override
  public NessieIdHasher hash(int value) {
    hashing.putInt(value);
    return this;
  }

  @Override
  public NessieIdHasher hash(long value) {
    hashing.putLong(value);
    return this;
  }

  @Override
  public NessieIdHasher hash(float value) {
    hashing.putFloat(value);
    return this;
  }

  @Override
  public NessieIdHasher hash(double value) {
    hashing.putDouble(value);
    return this;
  }

  @Override
  public NessieIdHasher hash(Enum<?> value) {
    if (value != null) {
      hashing.putInt(value.ordinal());
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(Boolean value) {
    if (value != null) {
      hashing.putBoolean(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(Integer value) {
    if (value != null) {
      hashing.putInt(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(Long value) {
    if (value != null) {
      hashing.putLong(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(Float value) {
    if (value != null) {
      hashing.putFloat(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(Double value) {
    if (value != null) {
      hashing.putDouble(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(String value) {
    if (value != null) {
      hashing.putString(value, UTF_8);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(byte[] value) {
    if (value != null) {
      hashing.putBytes(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(ByteBuffer value) {
    if (value != null) {
      hashing.putBytes(value);
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(UUID value) {
    if (value != null) {
      hashing.putLong(value.getMostSignificantBits());
      hashing.putLong(value.getLeastSignificantBits());
    }
    return this;
  }

  @Override
  public NessieIdHasher hash(Hashable value) {
    if (value != null) {
      value.hash(this);
    }
    return this;
  }

  @Override
  public NessieIdHasher hashCollection(Collection<? extends Hashable> value) {
    if (value != null) {
      value.forEach(this::hash);
    }
    return this;
  }

  @Override
  public NessieIdHasher hashUuidCollection(Collection<UUID> value) {
    if (value != null) {
      value.forEach(
          uuid -> hash(uuid.getMostSignificantBits()).hash(uuid.getLeastSignificantBits()));
    }
    return this;
  }

  @Override
  public NessieIdHasher hashIntCollection(Collection<Integer> value) {
    if (value != null) {
      value.forEach(this::hash);
    }
    return this;
  }

  @Override
  public NessieIdHasher hashLongCollection(Collection<Long> value) {
    if (value != null) {
      value.forEach(this::hash);
    }
    return this;
  }

  @Override
  public NessieIdHasher hashStringToStringMap(Map<String, String> map) {
    if (map != null) {
      map.entrySet().stream()
          .sorted(comparingByKey())
          .forEach(e -> hash(e.getKey()).hash(e.getValue()));
    }
    return this;
  }

  @Override
  public NessieId generate() {
    return NessieId.nessieIdFromBytes(hashing.hash().asBytes());
  }
}
