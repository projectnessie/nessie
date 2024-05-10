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
package org.projectnessie.catalog.model.snapshot;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.model.Content;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Represents the state of a {@link NessieView} on a specific Nessie reference (commit).
 *
 * <p>The {@linkplain #id() ID of a table's snapshot} in the Nessie catalog is derived from relevant
 * fields in a concrete {@linkplain Content Nessie content object}, for example a {@link
 * IcebergTable} or {@link DeltaLakeTable}. This guarantees that each distinct state of a table is
 * represented by exactly one {@linkplain NessieViewSnapshot snapshot}. How exactly the {@linkplain
 * #id() ID} is derived is opaque to a user.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieViewSnapshot.class)
@JsonDeserialize(as = ImmutableNessieViewSnapshot.class)
@JsonTypeName("VIEW")
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface NessieViewSnapshot extends NessieEntitySnapshot<NessieView> {

  @Override
  NessieViewSnapshot withId(NessieId id);

  @Override
  @Value.NonAttribute
  default String type() {
    return "VIEW";
  }

  /** The snapshots of tables and views referenced by this view. */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieViewDependency> dependencies();

  @Override
  NessieView entity();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Long icebergCurrentVersionId();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> icebergVersionSummary();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergDefaultCatalog();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  List<String> icebergNamespace();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieViewRepresentation> representations();

  static Builder builder() {
    return ImmutableNessieViewSnapshot.builder();
  }

  @SuppressWarnings("unused")
  interface Builder extends NessieEntitySnapshot.Builder<Builder> {
    @CanIgnoreReturnValue
    Builder from(NessieViewSnapshot instance);

    @CanIgnoreReturnValue
    Builder entity(NessieView entity);

    @CanIgnoreReturnValue
    Builder addDependency(NessieViewDependency element);

    @CanIgnoreReturnValue
    Builder addDependencies(NessieViewDependency... elements);

    @CanIgnoreReturnValue
    Builder dependencies(Iterable<? extends NessieViewDependency> elements);

    @CanIgnoreReturnValue
    Builder addAllDependencies(Iterable<? extends NessieViewDependency> elements);

    @CanIgnoreReturnValue
    Builder icebergCurrentVersionId(@Nullable Long icebergCurrentVersionId);

    @CanIgnoreReturnValue
    Builder putIcebergVersionSummary(String key, String value);

    @CanIgnoreReturnValue
    Builder putIcebergVersionSummary(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder icebergVersionSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllIcebergVersionSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder icebergDefaultCatalog(String icebergDefaultCatalog);

    @CanIgnoreReturnValue
    Builder icebergNamespace(@Nullable Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addRepresentation(NessieViewRepresentation element);

    @CanIgnoreReturnValue
    Builder addRepresentations(NessieViewRepresentation... elements);

    @CanIgnoreReturnValue
    Builder representations(Iterable<? extends NessieViewRepresentation> elements);

    @CanIgnoreReturnValue
    Builder addAllRepresentations(Iterable<? extends NessieViewRepresentation> elements);

    NessieViewSnapshot build();
  }
}
