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
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.Hashable;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessiePartitionField.class)
@JsonDeserialize(as = ImmutableNessiePartitionField.class)
public interface NessiePartitionField extends Hashable {
  int NO_FIELD_ID = -1;

  UUID id();

  UUID sourceFieldId();

  String name();

  // FIXME there is already a type() method in NessieField, why do we need it here?
  NessieTypeSpec type();

  @JsonSerialize(using = NessieFieldTransformSerializer.class)
  @JsonDeserialize(using = NessieFieldTransformDeserializer.class)
  NessieFieldTransform transformSpec();

  // TODO Iceberg specific
  // TODO do we want to permanently associate a field to an Iceberg column-ID?
  @Value.Default
  default int icebergId() {
    return NO_FIELD_ID;
  }

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(id());
  }

  static NessiePartitionField nessiePartitionField(
      UUID id,
      UUID sourceFieldId,
      String name,
      NessieTypeSpec type,
      NessieFieldTransform transformSpec,
      int icebergFieldId) {
    return ImmutableNessiePartitionField.of(
        id, sourceFieldId, name, type, transformSpec, icebergFieldId);
  }

  static Builder builder() {
    return ImmutableNessiePartitionField.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessiePartitionField instance);

    @CanIgnoreReturnValue
    Builder id(UUID id);

    @CanIgnoreReturnValue
    Builder sourceFieldId(UUID sourceFieldId);

    @CanIgnoreReturnValue
    Builder name(String name);

    @CanIgnoreReturnValue
    Builder type(NessieTypeSpec type);

    @CanIgnoreReturnValue
    Builder transformSpec(NessieFieldTransform transformSpec);

    @CanIgnoreReturnValue
    Builder icebergId(int icebergId);

    NessiePartitionField build();
  }
}
