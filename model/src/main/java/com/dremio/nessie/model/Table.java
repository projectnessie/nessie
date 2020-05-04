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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

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

  public abstract String getTableName();

  @Nullable
  public abstract String getNamespace();

  public abstract String getMetadataLocation();

  public abstract String getId();

  @Value.Default
  public boolean isDeleted() {
    return false;
  }

  @Value.Default
  public long getUpdateTime() {
    return Long.MIN_VALUE;
  }

  @Nullable
  public abstract TableMeta getMetadata();
}
