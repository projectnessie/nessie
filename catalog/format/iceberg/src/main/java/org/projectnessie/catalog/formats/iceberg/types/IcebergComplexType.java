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
package org.projectnessie.catalog.formats.iceberg.types;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type",
    include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(IcebergStructType.class),
  @JsonSubTypes.Type(IcebergListType.class),
  @JsonSubTypes.Type(IcebergMapType.class)
})
public interface IcebergComplexType extends IcebergType {
  @Override
  default int compare(Object left, Object right) {
    throw new UnsupportedOperationException();
  }

  @Override
  default byte[] serializeSingleValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  default Object deserializeSingleValue(byte[] value) {
    throw new UnsupportedOperationException();
  }
}
