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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(
    as = String.class,
    using = IcebergTypes.IcebergPrimitiveSerializer.class,
    typing = JsonSerialize.Typing.DYNAMIC)
@JsonDeserialize(as = String.class, using = IcebergTypes.IcebergPrimitiveDeserializer.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class IcebergPrimitiveType implements IcebergType {

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public int compare(Object left, Object right) {
    return ((Comparable) left).compareTo(right);
  }

  @Override
  public int hashCode() {
    return type().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    return obj.getClass().isAssignableFrom(this.getClass());
  }

  @Override
  public String toString() {
    return type();
  }
}
