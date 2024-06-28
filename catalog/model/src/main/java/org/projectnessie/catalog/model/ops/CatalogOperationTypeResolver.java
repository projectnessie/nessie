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

import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;
import org.projectnessie.model.Content;

/**
 * Represents a pluggable component that supports certain {@link CatalogOperation} subclasses for
 * specific {@link Content.Type}s.
 */
public interface CatalogOperationTypeResolver {
  /**
   * Returns the java object class representing {@link CatalogOperation}s for the given content
   * type, or {@code null} if this resolver does not support the content type.
   */
  @Nullable
  Class<? extends CatalogOperation> forContentType(@NotNull Content.Type type);
}
