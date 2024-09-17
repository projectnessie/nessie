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
package org.projectnessie.events.service.catalog;

import java.security.Principal;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.ops.CatalogOperationResult;
import org.projectnessie.nessie.immutables.NessieImmutable;

/** An internal event triggered when the catalog updates an entity. */
@NessieImmutable
@Value.Style(optionalAcceptNullable = true)
public interface CatalogOperationResultEvent {

  /** The repository id affected by the change. Never null, but may be an empty string. */
  String getRepositoryId();

  /** The user principal that initiated the change. May be empty if authentication is disabled. */
  Optional<Principal> getUser();

  /** The {@link CatalogOperationResult} produced by the catalog operation. */
  CatalogOperationResult getCatalogOperationResult();
}
