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

import com.dremio.iceberg.backend.Backend;
import com.dremio.iceberg.backend.EntityBackend;
import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tag;
import com.dremio.iceberg.model.User;
import com.dremio.iceberg.model.VersionedWrapper;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * basic class to demonstrate the backend model. WARNING do not use in production
 */
public class InMemory implements Backend {

  private static final TableInMemory TABLE = new TableInMemory();
  private static final TagInMemory TAG = new TagInMemory();

  @Override
  public EntityBackend<Table> tableBackend() {
    return TABLE;
  }

  @Override
  public EntityBackend<Tag> tagBackend() {
    return TAG;
  }

  @Override
  public EntityBackend<User> userBackend() {
    throw new UnsupportedOperationException("cant use in memory backend with database");
  }

  public static void clear() {
    TABLE.TABLES.clear();
    TAG.TAGS.clear();
  }

  /**
   * table backend. Do not use in production
   */
  public static class TableInMemory implements EntityBackend<Table> {

    static final Map<String, VersionedWrapper<Table>> TABLES = Maps.newHashMap();

    public TableInMemory() {
    }

    @Override
    public VersionedWrapper<Table> get(String name) {
      return TABLES.get(name);
    }

    @Override
    public List<VersionedWrapper<Table>> getAll(String name,
                                                String namespace,
                                                boolean includeDeleted) {

      return TABLES.values().stream()
                   .filter(t -> includeDeleted || !t.getObj().isDeleted())
                   .filter(t -> {
                     if (namespace == null) {
                       return true;
                     }
                     return Objects.equals(namespace, t.getObj().getNamespace());
                   })
                   .filter(t -> {
                     if (name == null) {
                       return true;
                     }
                     return Objects.equals(name, t.getObj().getTableName());
                   })
                   .collect(Collectors.toList());
    }

    @Override
    public void create(String name, VersionedWrapper<Table> table) {
      if (TABLES.containsKey(name)) {
        throw new UnsupportedOperationException("Table " + name + " already exists");
      }
      TABLES.put(name, increment(table));
    }

    @Override
    public void update(String name, VersionedWrapper<Table> table) {
      TABLES.put(name, increment(table));
    }

    @Override
    public void remove(String name) {
      TABLES.remove(name);
    }

  }

  /**
   * tag backend. Do not use in production
   */
  public static class TagInMemory implements EntityBackend<Tag> {

    private static final Map<String, VersionedWrapper<Tag>> TAGS = Maps.newHashMap();

    public TagInMemory() {
    }

    @Override
    public VersionedWrapper<Tag> get(String name) {
      return TAGS.get(name);
    }

    @Override
    public List<VersionedWrapper<Tag>> getAll(String name,
                                              String namespace,
                                              boolean includeDeleted) {

      return TAGS.values().stream()
                 .filter(t -> includeDeleted || !t.getObj().isDeleted())
                 .filter(t -> name == null || Objects.equals(name, t.getObj().getName()))
                 .collect(Collectors.toList());
    }

    @Override
    public void create(String name, VersionedWrapper<Tag> tag) {
      if (TAGS.containsKey(name)) {
        throw new UnsupportedOperationException("Table " + name + " already exists");
      }
      TAGS.put(name, increment(tag));
    }

    @Override
    public void update(String name, VersionedWrapper<Tag> tag) {
      TAGS.put(name, increment(tag));
    }

    @Override
    public void remove(String name) {
      TAGS.remove(name);
    }

  }
}
