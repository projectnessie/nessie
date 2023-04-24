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
package org.projectnessie.events.api;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.immutables.value.Value;

/** An object stored in Nessie, such as a table or a view. */
@JsonSubTypes({
  @JsonSubTypes.Type(name = "ICEBERG_TABLE", value = ImmutableIcebergTable.class),
  @JsonSubTypes.Type(name = "ICEBERG_VIEW", value = ImmutableIcebergView.class),
  @JsonSubTypes.Type(name = "NAMESPACE", value = ImmutableNamespace.class),
  @JsonSubTypes.Type(name = "DELTA_LAKE_TABLE", value = ImmutableDeltaLakeTable.class),
  @JsonSubTypes.Type(name = "CUSTOM", value = ImmutableCustomContent.class),
})
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
public interface Content {

  ContentType getType();

  /**
   * Unique id for this object.
   *
   * <p>This id is unique for the entire lifetime of this Content object and persists across
   * renames. Two content objects with the same key will have different ids.
   */
  UUID getId();

  /** A map of attributes that can be used to add additional information to the content object. */
  @Value.Default
  default Map<String, Object> getAttributes() {
    return Collections.emptyMap();
  }
}
