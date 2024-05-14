/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;
import org.projectnessie.model.ser.Views;

@Value.Immutable
@JsonSerialize(as = ImmutableGetNamespacesResponse.class)
@JsonDeserialize(as = ImmutableGetNamespacesResponse.class)
public interface GetNamespacesResponse {
  @NotNull
  @jakarta.validation.constraints.NotNull
  List<Namespace> getNamespaces();

  /**
   * The effective reference (for example a branch or tag) including the commit ID from which the
   * namespaces were fetched. Cannot be null when using REST API v2.
   */
  @JsonView(Views.V2.class)
  @Nullable // Only nullable in V1
  @jakarta.annotation.Nullable
  Reference getEffectiveReference();

  static ImmutableGetNamespacesResponse.Builder builder() {
    return ImmutableGetNamespacesResponse.builder();
  }
}
