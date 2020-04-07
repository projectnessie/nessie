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

package com.dremio.iceberg.client;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iceberg.catalog.TableIdentifier;

import com.dremio.iceberg.client.tag.CopyTag;
import com.dremio.iceberg.client.tag.Tag;
import com.dremio.iceberg.client.tag.UpdateTags;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.TableVersion;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Iceberg Alley representation of Iceberg Tag
 */
public class AlleyTag implements Tag {
  private static final Joiner DOT = Joiner.on('.');
  private final AlleyClient client;
  private final Map<TableIdentifier, Table> tableCache = Maps.newHashMap();
  private com.dremio.iceberg.model.Tag tag;

  public AlleyTag(com.dremio.iceberg.model.Tag tag, AlleyClient client) {
    this.tag = tag;
    this.client = client;

  }

  @Override
  public String name() {
    return (tag == null) ? null : tag.getName();
  }

  @Override
  public void refresh() {
    if (tag != null) {
      this.tag = client.getTagClient().getObject(tag.getId());
    }
    tableCache.clear();
  }

  @Override
  public void commit(Tag copy) {
    com.dremio.iceberg.model.Tag newTag = tag.withSnapshots(
      copy.tables().stream().map(t -> setTableVersion(t, copy.getMetadataLocation(t))).collect(Collectors.toList())
    );
    client.getTagClient().updateObject(newTag);
    refresh();
  }

  private Table getTable(TableIdentifier tableIdentifier) {
    return tableCache.computeIfAbsent(tableIdentifier,
      t -> client.getTableClient().getObjectByName(tableIdentifier.name(), tableIdentifier.namespace().toString()));
  }

  @Override
  public String getMetadataLocation(TableIdentifier tableIdentifier) {
    Table table = getTable(tableIdentifier);
    if (table == null) {
      return null;
    }
    TableVersion tableVersion = getTableVersion(table);
    if (tableVersion != null) {
      return tableVersion.getMetadataLocation();
    }
    return table.getMetadataLocation();
  }

  private TableVersion setTableVersion(TableIdentifier tableIdentifier, String currentAlleyTable) {
    Table alleyTable = getTable(tableIdentifier);
    TableVersion version = alleyTable.getTableVersion(currentAlleyTable);
    if (version == null) {
      throw new RuntimeException(); //todo
    }
    return version;
  }

  @Override
  public List<TableIdentifier> tables() {
    return tag.tableSnapshotKeys()
      .stream()
      .map(s -> client.getTableClient().getObject(s))
      .map(t -> TableIdentifier.parse(DOT.join(t.getNamespace(), t.getTableName())))
      .collect(Collectors.toList());
  }

  @Override
  public UpdateTags updateTags() {
    refresh();
    return new UpdateTags(this);
  }

  @Override
  public CopyTag copyTags() {
    refresh();
    return new CopyTag(this);
  }

  @Override
  public boolean isValid() {
    return tag != null;
  }

  private TableVersion getTableVersion(Table table) {
    if (tag == null) {
      return null; //todo
    }
    TableVersion tableVersion = tag.getTableSnapshot(table.getId());
    if (tableVersion == null) {
      return null; //todo
    }
    boolean exists = table.tableVersions().stream().map(table::getTableVersion).anyMatch(t -> t.equals(tableVersion));
    if (!exists) {
      return null; //todo
    }
    return tableVersion;
  }
}
