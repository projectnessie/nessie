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
package com.dremio.iceberg.client.tag;

import java.util.Map;

import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

import com.google.common.collect.Maps;

public class UpdateTags implements PendingUpdate<Tag> {

  private final Tag tag;
  private final Map<TableIdentifier, TableOperations> updates = Maps.newHashMap();

  public UpdateTags(Tag tag) {
    this.tag = tag;
  }

  public UpdateTags updateTable(TableIdentifier tableIdentifier, TableOperations table) {
    updates.put(tableIdentifier, table);
    return this;
  }

  @Override
  public Tag apply() {
    return tag;
  }

  @Override
  public void commit() {
    if (tag == null || !tag.isValid()) {
      return;
    }
    for (Map.Entry<TableIdentifier, TableOperations> entry: updates.entrySet()) {
      tag.setTableVersion(entry.getKey(), entry.getValue());
    }
    try {
      tag.commit();
    } finally {
      tag.refresh(); // todo if it fails we need to ensure we get old data back
    }
  }
}
