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

package com.dremio.iceberg.backend.simple;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tag;
import com.dremio.iceberg.model.User;
import com.google.common.collect.Maps;

public class InMemory implements Backend {

  @Override
  public EntityBackend<Table> tableBackend() {
    return new TableInMemory();
  }

  @Override
  public EntityBackend<Tag> tagBackend() {
    return new TagInMemory();
  }

  @Override
  public EntityBackend<User> userBackend() {
    throw new UnsupportedOperationException("cant use in memory backend with database");
  }

  public static class TableInMemory implements EntityBackend<Table> {
    private static final Map<String, Table> tables = Maps.newHashMap();

    public TableInMemory() {
    }

    @Override
    public Table get(String name) {
      return tables.get(name);
    }

    @Override
    public List<Table> getAll(boolean includeDeleted) {
      return getAll(null, includeDeleted);
    }

    @Override
    public List<Table> getAll(String namespace, boolean includeDeleted) {

      return tables.values().stream()
        .filter(t -> includeDeleted || !t.isDeleted())
        .filter(t -> {
          if (namespace == null) {
            return true;
          }
          return StringUtils.compare(namespace, t.getNamespace()) == 0;
        })
        .collect(Collectors.toList());
    }

    @Override
    public void create(String name, Table table) {
      if (tables.containsKey(name)) {
        throw new UnsupportedOperationException("Table " + name + " already exists");
      }
      table.setVersion(1L);
      tables.put(name, table);
    }

    @Override
    public void update(String name, Table table) {
      table.setVersion(table.getVersion()+1);
      tables.put(name, table);
    }

    @Override
    public void remove(String name) {
      tables.remove(name);
    }

  }

  public static class TagInMemory implements EntityBackend<Tag> {
    private static final Map<String, Tag> tables = Maps.newHashMap();

    public TagInMemory() {
    }

    @Override
    public Tag get(String name) {
      return tables.get(name);
    }

    @Override
    public List<Tag> getAll(boolean includeDeleted) {
      return getAll(null, includeDeleted);
    }

    @Override
    public List<Tag> getAll(String namespace, boolean includeDeleted) {

      return tables.values().stream()
        .filter(t -> includeDeleted || !t.isDeleted())
        .collect(Collectors.toList());
    }

    @Override
    public void create(String name, Tag table) {
      if (tables.containsKey(name)) {
        throw new UnsupportedOperationException("Table " + name + " already exists");
      }
      tables.put(name, table);
    }

    @Override
    public void update(String name, Tag table) {
      tables.put(name, table);
    }

    @Override
    public void remove(String name) {
      tables.remove(name);
    }

  }
}
