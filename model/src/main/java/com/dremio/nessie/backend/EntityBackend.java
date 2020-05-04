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

package com.dremio.nessie.backend;


import com.dremio.nessie.model.VersionedWrapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * Backend for a single type of API object (eg Tag/Table/User etc).
 *
 * <p>
 *   This class handles all database backend operations with the Server for a particular
 *   data type. The database is expected to provide strong consistency and to keep a version
 *   attribute to facilitate optimistic locking of individual records.
 *   {@link com.dremio.nessie.model.VersionedWrapper} holds the data object and the optimistic
 *   locking attribute as a pair.
 * </p>
 *
 * @param <T> The entity type this backend handles
 */
public interface EntityBackend<T> extends AutoCloseable {

  /**
   * Get entity for name.
   */
  VersionedWrapper<T> get(String name);

  /**
   * Get entity for name, optionally with sort key.
   */
  default VersionedWrapper<T> get(String name, String sortKey) {
    return get(name);
  }

  /**
   * Get all entities in database, optionally include deleted.
   *
   * <p>
   *   Here deleted refers to items that have been deleted logically by the application
   *   but have not been purged from the database.
   * </p>
   */
  List<VersionedWrapper<T>> getAll(boolean includeDeleted);

  default VersionedWrapper<T> increment(VersionedWrapper<T> obj) {
    return obj.increment();
  }


  /**
   * Create or update a record in the database.
   *
   * @param name name of entity. Possibly ignored by database implementation.
   * @param table Object to store. The version is expected to be the most recent known version.
   * @return updated object with updated version.
   */
  VersionedWrapper<T> update(String name, VersionedWrapper<T> table);

  /**
   * Batch update/create.
   *
   * <p>
   *   default implementation is to update each record at a time. Databases which support batch
   *   updates can override this method.
   * </p>
   * @param transaction set of Key/Object pairs to be stored.
   */
  default void updateAll(Map<String, VersionedWrapper<T>> transaction) {
    transaction.forEach(this::update);
  }

  /**
   * Completely remove a record given by name from the database.
   */
  void remove(String name);

  /**
   * Completely remove a record given by name from the database. Optionally with sort key.
   */
  default void remove(String target, String sortKey) {
    get(target);
  }
}
