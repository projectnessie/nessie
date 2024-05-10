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
package org.projectnessie.catalog.service.rest;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Holds the <em>effective</em> reference to a Nessie {@link ContentKey}.
 *
 * <p>References (branches, tags, detached commit IDs) can come from (first match wins):
 *
 * <ol>
 *   <li>the {@link IcebergTableIdentifier}, encoded in the {@link IcebergTableIdentifier#name()},
 *       for example as {@code my.namespace.`table-name@my-branch#SOME_COMMIT_ID`},
 *   <li>the {@code prefix} REST path parameter, see for example the {@code @Path} annotation of
 *       {@link IcebergApiV1TableResource#loadTable(String, String, String, String, String)},
 *   <li>the default branch
 * </ol>
 *
 * <p>Warehouse name can come from:
 *
 * <ol>
 *   <li>the {@code prefix} REST path parameter, see for example the {@code @Path} annotation of
 *       {@link IcebergApiV1TableResource#loadTable(String, String, String, String, String)},
 *   <li>the default warehouse
 * </ol>
 *
 * @see org.projectnessie.model.TableReference
 */
@NessieImmutable
public interface TableRef {
  @Value.Parameter(order = 1)
  ContentKey contentKey();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  ParsedReference reference();

  @Value.Parameter(order = 4)
  @Nullable
  @jakarta.annotation.Nullable
  String warehouse();

  static TableRef tableRef(ContentKey contentKey, ParsedReference reference, String warehouse) {
    return ImmutableTableRef.of(contentKey, reference, warehouse);
  }
}
