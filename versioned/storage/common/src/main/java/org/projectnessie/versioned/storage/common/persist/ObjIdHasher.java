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
package org.projectnessie.versioned.storage.common.persist;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface ObjIdHasher {
  static ObjIdHasher objIdHasher(String typeName) {
    return new ObjIdHasherImpl().hash(typeName);
  }

  static ObjIdHasher objIdHasher(Enum<?> enumValue) {
    return objIdHasher(enumValue.name());
  }

  ObjIdHasher hash(boolean value);

  ObjIdHasher hash(char value);

  ObjIdHasher hash(int value);

  ObjIdHasher hash(long value);

  ObjIdHasher hash(float value);

  ObjIdHasher hash(double value);

  ObjIdHasher hash(Enum<?> value);

  ObjIdHasher hash(Boolean value);

  ObjIdHasher hash(Integer value);

  ObjIdHasher hash(Long value);

  ObjIdHasher hash(Float value);

  ObjIdHasher hash(Double value);

  ObjIdHasher hash(String value);

  ObjIdHasher hash(byte[] value);

  ObjIdHasher hash(ByteBuffer value);

  ObjIdHasher hash(UUID value);

  ObjIdHasher hash(Hashable value);

  ObjIdHasher hash(ObjId value);

  ObjIdHasher hashCollection(Collection<? extends Hashable> value);

  ObjIdHasher hashUuidCollection(Collection<UUID> value);

  ObjIdHasher hashIntCollection(Collection<Integer> value);

  ObjIdHasher hashLongCollection(Collection<Long> value);

  ObjIdHasher hashStringToStringMap(Map<String, String> map);

  ObjId generate();
}
