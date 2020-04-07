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

import com.dremio.iceberg.client.tag.CopyTag;
import com.dremio.iceberg.client.tag.Tag;
import com.dremio.iceberg.client.tag.UpdateTag;
import com.dremio.iceberg.model.ImmutableTag;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.TableVersion;
import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Iceberg Alley representation of Iceberg Tag.
 */
public class AlleyTag implements Tag {

  private static final Joiner DOT = Joiner.on('.');
  private final AlleyClient client;
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
  }

  @Override
  public void commit(Tag old) {
    Map<String, TableVersion> snapshots = new HashMap<>();
    for (TableIdentifier t : old.tables()) {
      TableVersion tableVersion = setTableVersion(t, old.getMetadataLocation(t));
      if (snapshots.put(tableVersion.getUuid(), tableVersion) != null) {
        throw new IllegalStateException("Duplicate key");
      }
    }
    Map<String, TableVersion> newSnapshots = new HashMap<>(tag.getTableVersions());
    newSnapshots.putAll(snapshots);
    com.dremio.iceberg.model.Tag newTag = ImmutableTag.builder().from(tag)
                                                      .tableVersions(newSnapshots)
                                                      .build();
    client.getTagClient().updateObject(newTag);
    refresh();
  }

  private Table getTable(TableIdentifier tableIdentifier) {
    return client.getTableClient()
                 .getObjectByName(tableIdentifier.name(),
                                  tableIdentifier.namespace()
                                                 .toString());
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
    TableVersion version = alleyTable.getVersionList().get(currentAlleyTable);
    if (version == null) {
      throw new RuntimeException(); //todo
    }
    return version;
  }

  @Override
  public List<TableIdentifier> tables() {
    return tag.getTableVersions()
              .keySet()
              .stream()
              .map(s -> client.getTableClient().getObject(s))
              .map(AlleyTag::tableIdentifier)
              .collect(Collectors.toList());
  }

  @Override
  public UpdateTag updateTags() {
    refresh();
    return new UpdateTag(this);
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
    TableVersion tableVersion = tag.getTableVersions().get(table.getId());
    if (tableVersion == null) {
      return null; //todo
    }
    boolean exists = table.getVersionList().values().stream()
                          .anyMatch(t -> t.equals(tableVersion));
    if (!exists) {
      return null; //todo
    }
    return tableVersion;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", AlleyTag.class.getSimpleName() + "[", "]")
      .add("client=" + client)
      .add("tag=" + tag)
      .toString();
  }

  private static TableIdentifier tableIdentifier(Table table) {
    if (table.getTableName() == null) {
      throw new UnsupportedOperationException("table name can't be null");
    }
    if (table.getNamespace() == null) {
      return TableIdentifier.of(table.getTableName());
    }
    return TableIdentifier.parse(DOT.join(table.getNamespace(), table.getTableName()));
  }
}
