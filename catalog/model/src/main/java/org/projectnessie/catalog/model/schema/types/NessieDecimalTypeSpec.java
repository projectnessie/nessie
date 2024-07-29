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

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieDecimalTypeSpec.class)
@JsonDeserialize(as = ImmutableNessieDecimalTypeSpec.class)
public interface NessieDecimalTypeSpec extends NessieTypeSpec {
  int scale();

  int precision();

  @Override
  default NessieType type() {
    return NessieType.DECIMAL;
  }

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(type().lowerCaseName()).hash(scale()).hash(precision());
  }

  default StringBuilder asString(StringBuilder targetBuffer) {
    return targetBuffer
        .append("decimal(")
        .append(precision())
        .append(",")
        .append(scale())
        .append(")");
  }

  @Value.Check
  default void check() {
    checkState(scale() >= 0, "Invalid scale %s for %s", scale(), type().lowerCaseName());
    checkState(
        precision() >= 0, "Invalid precision %s for %s", precision(), type().lowerCaseName());
  }
}
