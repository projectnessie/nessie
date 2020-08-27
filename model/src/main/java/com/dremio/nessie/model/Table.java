/*
 * Copyright (C) 2020 Dremio
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

package com.dremio.nessie.model;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Abstract implementation of a Table in Nessie.
 *
 * <p>
 *   At the least this table must have an id and an identifying metadata location.
 *   The table metadata is optional and is currently only needed by the UI.
 * </p>
 */
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableTable.class)
@JsonDeserialize(as = ImmutableTable.class)
public abstract class Table implements Base {

  /**
   * Table Name.
   */
  public abstract String getName();

  /**
   * Optional Namespace.
   */
  @Nullable
  public abstract String getNamespace();

  /**
   * Location on a filesystem (defined by source system) of the current table metadata.
   */
  public abstract String getMetadataLocation();

  /**
   * Backend id of this table. Currently defined as full namespace.table.
   */
  public abstract String getId();

  /**
   * Optional flag to denote deletion on the source system. May still be present in Nessie db.
   */
  @Value.Default
  public boolean isDeleted() {
    return false;
  }

  /**
   * Milliseconds since epoch of the last time this table was updated.
   */
  @Value.Default
  public long getUpdateTime() {
    return Long.MIN_VALUE;
  }

  /**
   * Source table metadata. A subset of metadata files required for rich UI.
   */
  @Nullable
  public abstract TableMeta getMetadata();
}
