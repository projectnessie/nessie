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
package org.projectnessie.catalog.model.ops;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import jakarta.annotation.Nullable;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

/**
 * Common interface for change operations on a catalog. Each content type will have its changes
 * represented by specific subclasses, loaded via {@link CatalogOperationTypeIdResolver}.
 */
@JsonTypeIdResolver(CatalogOperationTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", visible = true)
public interface CatalogOperation {

  ContentKey getKey();

  Content.Type getType();

  /**
   * The logical warehouse name where this operation will occur. Must correspond to a warehouse
   * configured under {@code nessie.catalog.warehouses.<name>}.
   *
   * <p>If not set, the default warehouse will be used.
   */
  @Nullable
  String warehouse();
}
