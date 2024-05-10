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
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface NessieIdHasher {
  static NessieIdHasher nessieIdHasher(String typeName) {
    return new IdHasherImpl().hash(typeName);
  }

  NessieIdHasher hash(boolean value);

  NessieIdHasher hash(int value);

  NessieIdHasher hash(long value);

  NessieIdHasher hash(float value);

  NessieIdHasher hash(double value);

  NessieIdHasher hash(Enum<?> value);

  NessieIdHasher hash(Boolean value);

  NessieIdHasher hash(Integer value);

  NessieIdHasher hash(Long value);

  NessieIdHasher hash(Float value);

  NessieIdHasher hash(Double value);

  NessieIdHasher hash(String value);

  NessieIdHasher hash(byte[] value);

  NessieIdHasher hash(ByteBuffer value);

  NessieIdHasher hash(UUID value);

  NessieIdHasher hash(Hashable value);

  NessieIdHasher hashCollection(Collection<? extends Hashable> value);

  NessieIdHasher hashUuidCollection(Collection<UUID> value);

  NessieIdHasher hashIntCollection(Collection<Integer> value);

  NessieIdHasher hashLongCollection(Collection<Long> value);

  NessieIdHasher hashStringToStringMap(Map<String, String> map);

  NessieId generate();
}
