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
package org.projectnessie.catalog.service.rest;

import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergLoadTableResult;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Composite result of {@link IcebergConfigurer#icebergConfigPerTable(NessieEntitySnapshot, String,
 * String, Map, String, ContentKey, String, boolean)}.
 */
@NessieImmutable
interface IcebergTableConfig {
  /**
   * Optional, updated {@link IcebergTableMetadata#properties()} to be included in the {@link
   * IcebergLoadTableResult}.
   */
  Optional<Map<String, String>> updatedMetadataProperties();

  /** Values for {@link IcebergLoadTableResult#config()}. */
  Map<String, String> config();
}
