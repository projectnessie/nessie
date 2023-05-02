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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import java.util.Collections;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.events.api.json.ContentTypeIdResolver;

/**
 * An object stored in Nessie, such as a table or a view.
 *
 * @see Namespace
 * @see IcebergTable
 * @see IcebergView
 * @see DeltaLakeTable
 * @see GenericContent
 */
@JsonTypeIdResolver(ContentTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, visible = true, property = "type")
public interface Content {

  ContentType getType();

  /**
   * Unique id for this object.
   *
   * <p>This id is unique for the entire lifetime of this Content object and persists across
   * renames. Two content objects with the same key will have different ids.
   *
   * <p>Content ids are usually UUIDs, but this is not enforced currently in Nessie's model API.
   */
  String getId();

  /** A map of attributes that can be used to add additional information to the content object. */
  @Value.Default
  default Map<String, Object> getProperties() {
    return Collections.emptyMap();
  }
}
