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

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.io.FileIO;

/** SPI interface to abstract view metadata access and updates. */
public interface ViewOperations {

  /**
   * Return the currently loaded view metadata, without checking for updates.
   *
   * @return view version metadata
   */
  ViewVersionMetadata current();

  /**
   * Return extra properties not stored in {@link ViewVersionMetadata}.
   *
   * @return a map of properties
   */
  default Map<String, String> extraProperties() {
    return Collections.emptyMap();
  }

  /**
   * Return the current view metadata after checking for updates.
   *
   * @return view version metadata
   */
  ViewVersionMetadata refresh();

  /**
   * Drops the view specified by the 'viewIdentifier' parameter. Used only by the test
   * infrastructure such as HadoopViewOperations and TestViews. Metacat views are dropped using
   * direct hive 'drop' object calls.
   *
   * @param viewIdentifier specifies the name/location of the view.
   */
  void drop(String viewIdentifier);

  /**
   * Replace the base metadata with a new version.
   *
   * <p>This method should implement and document atomicity guarantees.
   *
   * <p>Implementations must check that the base metadata is current to avoid overwriting updates.
   * Once the atomic commit operation succeeds, implementations must not perform any operations that
   * may fail because failure in this method cannot be distinguished from commit failure.
   *
   * @param base view metadata on which changes were based
   * @param metadata new view metadata with updates
   * @param properties Version property genie-id of the operation, as well as table properties such
   *     as owner, table type, common view flag etc.
   */
  void commit(
      ViewVersionMetadata base, ViewVersionMetadata metadata, Map<String, String> properties);

  /**
   * A {@link FileIO} to read and write table data and metadata files.
   *
   * @return a {@link FileIO} to read and write table data and metadata files
   */
  FileIO io();
}
