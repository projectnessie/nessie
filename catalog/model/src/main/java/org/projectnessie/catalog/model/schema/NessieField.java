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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieField.class)
@JsonDeserialize(as = ImmutableNessieField.class)
public interface NessieField {
  int NO_COLUMN_ID = -1;

  static Builder builder() {
    return ImmutableNessieField.builder();
  }

  // TODO Is it possible that the same field/column (with the same Iceberg field/column-ID) is
  //  re-added to a schema? If yes, then the Nessie field ID needs to be deterministic.
  UUID id();

  // TODO Iceberg specific
  // TODO do we want to permanently associate a field to an Iceberg column-ID?
  @Value.Default
  default int icebergId() {
    return NO_COLUMN_ID;
  }

  String name();

  NessieTypeSpec type();

  // TODO Delta specific - can we just ignore metadata-columns entirely??
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  @Value.Default
  default boolean metadataColumn() {
    return false;
  }

  /** Opposite of {@link #metadataColumn()}. */
  @Value.NonAttribute
  default boolean dataColumn() {
    return !metadataColumn();
  }

  // TODO add a generic `NessieConstraint` type and make 'NULLABLE' a constraint?
  //  That way we can declare "any" kind of constraint for engines/table-formats that support that.
  // Delta way...
  boolean nullable();

  // TODO Delta has a generic property bag - not so great.
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> metadata();

  // TODO Iceberg embeds documentation into the schema. This isn't great for large docs though.
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String doc();

  @SuppressWarnings("unused")
  interface Builder {

    @CanIgnoreReturnValue
    Builder id(UUID id);

    @CanIgnoreReturnValue
    Builder icebergId(int icebergId);

    @CanIgnoreReturnValue
    Builder name(String name);

    @CanIgnoreReturnValue
    Builder type(NessieTypeSpec type);

    @CanIgnoreReturnValue
    Builder metadataColumn(boolean metadataColumn);

    @CanIgnoreReturnValue
    Builder nullable(boolean nullable);

    @CanIgnoreReturnValue
    Builder metadata(Map<String, ? extends String> metadataMap);

    @CanIgnoreReturnValue
    Builder doc(String doc);

    @CanIgnoreReturnValue
    Builder from(NessieField from);

    NessieField build();
  }
}
