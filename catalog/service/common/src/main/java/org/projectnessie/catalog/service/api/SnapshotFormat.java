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
package org.projectnessie.catalog.service.api;

/**
 * The output format of snapshot related entity information that is returned to actual clients, with
 * attributes whether additional entity metadata like schema, partition definition, sort definitions
 * and/or file-group entries are returned.
 */
public enum SnapshotFormat {
  /**
   * The Nessie Catalog main native format includes the entity snapshot information with schemas,
   * partition definitions and sort definitions.
   */
  NESSIE_SNAPSHOT,
  /** Iceberg table metadata. */
  ICEBERG_TABLE_METADATA,
}
