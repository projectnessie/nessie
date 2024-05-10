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

// TODO Delta: number specifying the number of days since 1970-01-01 - range: 0001-01-01 ..
//  9999-12-31
// TODO Delta: no timezone
// TODO Iceberg: ??
@NessieImmutable
@JsonSerialize(as = ImmutableNessieTimestampTypeSpec.class)
@JsonDeserialize(as = ImmutableNessieTimestampTypeSpec.class)
public interface NessieTimestampTypeSpec extends NessieTypeSpec {

  int precision();

  boolean withTimeZone();

  @Override
  default NessieType type() {
    return NessieType.TIMESTAMP;
  }

  default StringBuilder asString(StringBuilder targetBuffer) {
    return targetBuffer
        .append("timestamp(")
        .append(precision())
        .append(withTimeZone() ? ", with" : ", without")
        .append(" timezone)");
  }

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(type().lowerCaseName()).hash(precision()).hash(withTimeZone());
  }
}
