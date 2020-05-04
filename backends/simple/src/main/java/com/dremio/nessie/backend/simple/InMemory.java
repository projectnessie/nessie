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

package com.dremio.nessie.backend.simple;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.EntityBackend;
import com.dremio.nessie.model.GitObject;
import com.dremio.nessie.model.GitRef;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.model.Tag;
import com.dremio.nessie.model.User;
import com.dremio.nessie.model.VersionedWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * basic class to demonstrate the backend model. WARNING do not use in production.
 */
public class InMemory implements Backend {

  private final TableInMemory table = new TableInMemory();
  private final TagInMemory tag = new TagInMemory();
  private final RefInMemory ref = new RefInMemory();
  private final ObjectInMemory object = new ObjectInMemory();

  @Override
  public EntityBackend<Table> tableBackend() {
    return table;
  }

  @Override
  public EntityBackend<Tag> tagBackend() {
    return tag;
  }

  @Override
  public EntityBackend<User> userBackend() {
    throw new UnsupportedOperationException("cant use in memory backend with database");
  }

  @Override
  public EntityBackend<GitObject> gitBackend() {
    return object;
  }

  @Override
  public EntityBackend<GitRef> gitRefBackend() {
    return ref;
  }

  public void close() {
    table.close();
    tag.close();
  }

  private interface Filter<T> {

    boolean check(VersionedWrapper<T> obj, String name, String namespace, boolean includeDeleted);
  }

  public abstract static class InMemoryEntityBackend<T> implements EntityBackend<T> {

    private final Map<String, VersionedWrapper<T>> objects = new HashMap<>();

    @Override
    public VersionedWrapper<T> get(String name) {
      return objects.get(name);
    }

    protected abstract Filter<T> filterObj();

    @Override
    public List<VersionedWrapper<T>> getAll(String name,
                                            String namespace,
                                            boolean includeDeleted) {

      Filter<T> filter = filterObj();
      return objects.values().stream().filter(t -> filter.check(t, name, namespace, includeDeleted))
                    .collect(Collectors.toList());
    }

    @Override
    public void create(String name, VersionedWrapper<T> table) {
      objects.put(name, increment(table));
    }

    @Override
    public void update(String name, VersionedWrapper<T> table) {
      VersionedWrapper<T> current = objects.get(name);
      if (current == null) {
        create(name, table);
        current = objects.get(name);
      }

      assert current.getVersion().orElse(Long.MIN_VALUE)
             == table.getVersion().orElse(Long.MIN_VALUE);
      objects.put(name, increment(table));
    }

    @Override
    public void remove(String name) {
      objects.remove(name);
    }

    @Override
    public void close() {
      objects.clear();
    }
  }

  /**
   * table backend. Do not use in production.
   */
  public static class TableInMemory extends InMemoryEntityBackend<Table> {

    @Override
    protected Filter<Table> filterObj() {
      return (obj, name, namespace, includeDeleted) -> {
        if (!includeDeleted && obj.getObj().isDeleted()) {
          return false;
        }
        if (namespace != null && !Objects.equals(namespace, obj.getObj().getNamespace())) {
          return false;
        }
        if (name != null && !Objects.equals(name, obj.getObj().getTableName())) {
          return false;
        }
        return true;
      };
    }

  }

  /**
   * tag backend. Do not use in production.
   */
  public static class TagInMemory extends InMemoryEntityBackend<Tag> {

    @Override
    protected Filter<Tag> filterObj() {
      return (obj, name, namespace, includeDeleted) -> {
        if (!includeDeleted && obj.getObj().isDeleted()) {
          return false;
        }
        if (name != null && !Objects.equals(name, obj.getObj().getName())) {
          return false;
        }
        return true;
      };
    }
  }

  /**
   * object backend. Do not use in production.
   */
  public static class ObjectInMemory extends InMemoryEntityBackend<GitObject> {

    @Override
    protected Filter<GitObject> filterObj() {
      return (obj, name, namespace, includeDeleted) -> {
        if (!includeDeleted && obj.getObj().isDeleted()) {
          return false;
        }
        if (name != null && !Objects.equals(name, obj.getObj().getId())) {
          return false;
        }
        return true;
      };
    }
  }

  /**
   * ref backend. Do not use in production.
   */
  public static class RefInMemory extends InMemoryEntityBackend<GitRef> {

    @Override
    protected Filter<GitRef> filterObj() {
      return (obj, name, namespace, includeDeleted) -> {
        if (!includeDeleted && obj.getObj().isDeleted()) {
          return false;
        }
        if (name != null && !Objects.equals(name, obj.getObj().getId())) {
          return false;
        }
        return true;
      };
    }
  }
}
