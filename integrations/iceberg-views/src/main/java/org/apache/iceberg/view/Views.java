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
package org.apache.iceberg.view;

import java.util.Map;

/**
 * Generic interface for creating and loading a view implementation.
 *
 * <p>The 'viewIdentifier' field should be interpreted by the underlying implementation (e.g.
 * catalog.database.view_name)
 */
public interface Views {

  /**
   * Create a view without replacing any existing view.
   *
   * @param viewIdentifier view name or location
   * @param viewDefinition SQL metadata of the view
   * @param properties Version property genie-id of the operation, as well as table properties such
   *     as owner, table type, common view flag etc.
   */
  void create(String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties);

  /**
   * Replaces a view.
   *
   * @param viewIdentifier view name or location
   * @param viewDefinition SQL metadata of the view
   * @param properties Version property genie-id of the operation, as well as table properties such
   *     as owner, table type, common view flag etc.
   */
  void replace(
      String viewIdentifier, ViewDefinition viewDefinition, Map<String, String> properties);

  /**
   * Loads a view by name.
   *
   * @param viewIdentifier view name or location
   * @return All the metadata of the view
   */
  View load(String viewIdentifier);

  /**
   * Loads a view by name.
   *
   * @param viewIdentifier view name or location
   * @return SQL metadata of the view
   */
  ViewDefinition loadDefinition(String viewIdentifier);

  /**
   * Drops a view.
   *
   * @param viewIdentifier view name or location
   */
  void drop(String viewIdentifier);

  /**
   * Renames a view.
   *
   * @param oldIdentifier the view identifier of the existing view to rename
   * @param newIdentifier the new view identifier of the view
   */
  default void rename(String oldIdentifier, String newIdentifier) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
