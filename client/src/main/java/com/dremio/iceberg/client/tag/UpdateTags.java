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

import java.util.List;
import java.util.Map;

import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;

import com.dremio.iceberg.client.AlleyTableOperations;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * transaction object to update tags
 */
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
    try {
      tag.commit(new UpdateTag(updates));
    } finally {
      tag.refresh(); // todo if it fails we need to ensure we get old data back
    }
  }

  private class UpdateTag implements Tag {

    private final Map<TableIdentifier, TableOperations> updates;

    public UpdateTag(Map<TableIdentifier, TableOperations> updates) {
      this.updates = updates;
    }

    @Override
    public String name() {
      return null;
    }

    @Override
    public void refresh() {

    }

    @Override
    public void commit(Tag tag) {

    }

    @Override
    public String getMetadataLocation(TableIdentifier table) {
      return extractTableMetadata(updates.get(table));
    }

    @Override
    public List<TableIdentifier> tables() {
      return Lists.newArrayList(updates.keySet());
    }

    @Override
    public UpdateTags updateTags() {
      return null;
    }

    @Override
    public CopyTag copyTags() {
      return null;
    }

    @Override
    public boolean isValid() {
      return false;
    }
  }

  //todo we should not be publicly exposing IcebergAlley stuff.
  //Have to think about how to get the metadata location reliably w/o resorting to casting to iceberg
  //todo make alleyTable() non-public or remove completely after the above is done
  private static String extractTableMetadata(TableOperations table) {
    try {
      AlleyTableOperations alleyOps = (AlleyTableOperations) table;
      return alleyOps.alleyTable().getMetadataLocation();
    } catch (ClassCastException e) {
      throw new RuntimeException("Looks like you aren't using IcebergAlley, can't continue", e);
    }
  }
}
