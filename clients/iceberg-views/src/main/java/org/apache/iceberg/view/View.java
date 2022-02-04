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

import java.util.List;
import java.util.Map;

/** Interface for view definition. */
public interface View {

  /**
   * Get the current {@link Version version} for this view, or null if there are no versions.
   *
   * @return the current view version.
   */
  Version currentVersion();

  /**
   * Get the {@link Version versions} of this view.
   *
   * @return an Iterable of versions of this view.
   */
  Iterable<Version> versions();

  /**
   * Get a {@link Version version} in this view by ID.
   *
   * @param versionId version ID
   * @return a Version, or null if the ID cannot be found
   */
  Version version(int versionId);

  /**
   * Get the version history of this table.
   *
   * @return a list of {@link HistoryEntry}
   */
  List<HistoryEntry> history();

  /**
   * Return a map of string properties for this view.
   *
   * @return this view's properties map
   */
  Map<String, String> properties();

  /**
   * Update view properties and commit the changes.
   *
   * @return a new {@link UpdateProperties}
   */
  UpdateProperties updateProperties();
}
