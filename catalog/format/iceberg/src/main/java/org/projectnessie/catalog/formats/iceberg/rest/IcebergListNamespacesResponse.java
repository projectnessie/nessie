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
package org.projectnessie.catalog.formats.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergListNamespacesResponse.class)
@JsonDeserialize(as = ImmutableIcebergListNamespacesResponse.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergListNamespacesResponse {
  @Nullable
  String nextPageToken();

  List<IcebergNamespace> namespaces();

  static Builder builder() {
    return ImmutableIcebergListNamespacesResponse.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergListNamespacesResponse instance);

    @CanIgnoreReturnValue
    Builder nextPageToken(String nextPageToken);

    @CanIgnoreReturnValue
    Builder addNamespace(IcebergNamespace element);

    @CanIgnoreReturnValue
    Builder addNamespaces(IcebergNamespace... elements);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder namespaces(Iterable<? extends IcebergNamespace> elements);

    @CanIgnoreReturnValue
    Builder addAllNamespaces(Iterable<? extends IcebergNamespace> elements);

    IcebergListNamespacesResponse build();
  }
}
