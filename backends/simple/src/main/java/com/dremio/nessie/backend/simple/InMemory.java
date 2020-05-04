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
import com.dremio.nessie.model.BranchControllerObject;
import com.dremio.nessie.model.BranchControllerReference;
import com.dremio.nessie.model.User;
import com.dremio.nessie.model.VersionedWrapper;
import com.dremio.nessie.server.ServerConfiguration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * basic class to demonstrate the backend model. WARNING do not use in production.
 */
public class InMemory implements Backend {

  private final RefInMemory ref = new RefInMemory();
  private final ObjectInMemory object = new ObjectInMemory();

  @Override
  public EntityBackend<User> userBackend() {
    throw new UnsupportedOperationException("cant use in memory backend with database");
  }

  @Override
  public EntityBackend<BranchControllerObject> gitBackend() {
    return object;
  }

  @Override
  public EntityBackend<BranchControllerReference> gitRefBackend() {
    return ref;
  }

  public void close() {
    ref.close();
    object.close();
  }

  private interface Filter<T> {

    boolean check(VersionedWrapper<T> obj, boolean includeDeleted);
  }

  public abstract static class InMemoryEntityBackend<T> implements EntityBackend<T> {

    private final Map<String, VersionedWrapper<T>> objects = new HashMap<>();

    @Override
    public VersionedWrapper<T> get(String name) {
      return objects.get(name);
    }

    protected abstract Filter<T> filterObj();

    @Override
    public List<VersionedWrapper<T>> getAll(boolean includeDeleted) {

      Filter<T> filter = filterObj();
      return objects.values().stream().filter(t -> filter.check(t, includeDeleted))
                    .collect(Collectors.toList());
    }

    @Override
    public VersionedWrapper<T> update(String name, VersionedWrapper<T> table) {
      VersionedWrapper<T> current = objects.get(name);
      if (current == null) {
        objects.put(name, increment(table));
        current = objects.get(name);
      }

      if (!current.getVersion().equals(table.getVersion())) {
        throw new IllegalStateException("InMemory version incorrect");
      }
      objects.put(name, increment(table));
      return objects.get(name);
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
   * object backend. Do not use in production.
   */
  public static class ObjectInMemory extends InMemoryEntityBackend<BranchControllerObject> {

    @Override
    protected Filter<BranchControllerObject> filterObj() {
      return (obj, includeDeleted) -> true;
    }
  }

  /**
   * ref backend. Do not use in production.
   */
  public static class RefInMemory extends InMemoryEntityBackend<BranchControllerReference> {

    @Override
    protected Filter<BranchControllerReference> filterObj() {
      return (obj, includeDeleted) -> true;
    }
  }

  public static class BackendFactory implements Factory {

    @Override
    public Backend create(ServerConfiguration config) {
      return new InMemory();
    }
  }
}
