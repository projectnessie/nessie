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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergViewVersion.class)
@JsonDeserialize(as = ImmutableIcebergViewVersion.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergViewVersion {
  long versionId();

  IcebergViewVersion withVersionId(long versionId);

  long timestampMs();

  int schemaId();

  IcebergViewVersion withSchemaId(int schemaId);

  Map<String, String> summary();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  String defaultCatalog();

  IcebergNamespace defaultNamespace();

  List<IcebergViewRepresentation> representations();

  static Builder builder() {
    return ImmutableIcebergViewVersion.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergViewVersion instance);

    @CanIgnoreReturnValue
    Builder versionId(long versionId);

    @CanIgnoreReturnValue
    Builder timestampMs(long timestampMs);

    @CanIgnoreReturnValue
    Builder schemaId(int schemaId);

    @CanIgnoreReturnValue
    Builder putSummary(String key, String value);

    @CanIgnoreReturnValue
    Builder putSummary(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder summary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllSummary(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder defaultCatalog(@Nullable String defaultCatalog);

    @CanIgnoreReturnValue
    Builder defaultNamespace(IcebergNamespace defaultNamespace);

    @CanIgnoreReturnValue
    Builder addRepresentation(IcebergViewRepresentation element);

    @CanIgnoreReturnValue
    Builder addRepresentations(IcebergViewRepresentation... elements);

    @CanIgnoreReturnValue
    Builder representations(Iterable<? extends IcebergViewRepresentation> elements);

    @CanIgnoreReturnValue
    Builder addAllRepresentations(Iterable<? extends IcebergViewRepresentation> elements);

    IcebergViewVersion build();
  }
}
