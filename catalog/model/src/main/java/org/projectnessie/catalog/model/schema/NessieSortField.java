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
package org.projectnessie.catalog.model.schema;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.UUID;
import org.projectnessie.catalog.model.id.Hashable;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieSortField.class)
@JsonDeserialize(as = ImmutableNessieSortField.class)
public interface NessieSortField extends Hashable {

  UUID sourceFieldId();

  NessieTypeSpec type();

  @JsonSerialize(using = NessieFieldTransformSerializer.class)
  @JsonDeserialize(using = NessieFieldTransformDeserializer.class)
  NessieFieldTransform transformSpec();

  NessieNullOrder nullOrder();

  NessieSortDirection direction();

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher
        .hash(sourceFieldId())
        .hash(type())
        .hash(transformSpec())
        .hash(nullOrder().name())
        .hash(direction().name());
  }

  static Builder builder() {
    return ImmutableNessieSortField.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieSortField instance);

    @CanIgnoreReturnValue
    Builder sourceFieldId(UUID sourceFieldId);

    @CanIgnoreReturnValue
    Builder type(NessieTypeSpec type);

    @CanIgnoreReturnValue
    Builder transformSpec(NessieFieldTransform transformSpec);

    @CanIgnoreReturnValue
    Builder nullOrder(NessieNullOrder nullOrder);

    @CanIgnoreReturnValue
    Builder direction(NessieSortDirection direction);

    NessieSortField build();
  }
}
