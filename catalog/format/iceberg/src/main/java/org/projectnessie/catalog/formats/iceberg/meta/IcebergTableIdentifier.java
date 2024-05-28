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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergTableIdentifier.class)
@JsonDeserialize(as = ImmutableIcebergTableIdentifier.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergTableIdentifier {

  @Value.Parameter(order = 1)
  IcebergNamespace namespace();

  @Value.Parameter(order = 2)
  String name();

  static IcebergTableIdentifier fromNessieContentKey(ContentKey name) {
    return icebergTableIdentifier(
        IcebergNamespace.fromNessieNamespace(name.getNamespace()), name.getName());
  }

  static IcebergTableIdentifier icebergTableIdentifier(IcebergNamespace namespace, String name) {
    return ImmutableIcebergTableIdentifier.of(namespace, name);
  }

  default ContentKey toNessieContentKey() {
    return ContentKey.of(namespace().toNessieNamespace(), name());
  }

  static Builder builder() {
    return ImmutableIcebergTableIdentifier.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder namespace(IcebergNamespace namespace);

    @CanIgnoreReturnValue
    Builder name(String name);

    IcebergTableIdentifier build();
  }
}
