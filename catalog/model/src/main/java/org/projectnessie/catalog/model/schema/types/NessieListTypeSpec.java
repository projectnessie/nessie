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
package org.projectnessie.catalog.model.schema.types;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieListTypeSpec.class)
@JsonDeserialize(as = ImmutableNessieListTypeSpec.class)
public interface NessieListTypeSpec extends NessieTypeSpec {

  NessieTypeSpec elementType();

  // TODO do we want to permanently associate a field to an Iceberg column-ID?
  int icebergElementFieldId();

  boolean elementsNullable();

  @Override
  default NessieType type() {
    return NessieType.LIST;
  }

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher
        .hash(type().lowerCaseName())
        .hash(elementType())
        .hash(icebergElementFieldId())
        .hash(elementsNullable());
  }

  default StringBuilder asString(StringBuilder targetBuffer) {
    targetBuffer.append("list<");
    elementType().asString(targetBuffer);
    if (elementsNullable()) {
      targetBuffer.append(" null");
    }
    return targetBuffer.append(">");
  }
}
