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

package com.dremio.iceberg.client.tag;

import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * public interface for the Tag type in iceberg.
 */
public interface Tag {

  /**
   * this tag's name.
   */
  String name();

  /**
   * refresh this tag from the backend.
   */
  void refresh();

  /**
   * commit a transaction.
   *
   * <p>
   * The transaction is defined by the given tag. This tag has all updates/additions in it
   * this tags tables/versions are replaced with the given tags
   * </p>
   */
  void commit(Tag tag);

  /**
   * retrieve the metadata location for a given table.
   *
   * <p>
   *   todo normally we shouldn't expose metadata location, can it be hidden?
   * </p>
   */
  String getMetadataLocation(TableIdentifier table);

  /**
   * List all tables associated with this tag.
   */
  List<TableIdentifier> tables();

  /**
   * begin an UpdateTag transaction.
   *
   * <p>
   *   This will update a set of tables in this tag (or add new ones)
   * </p>
   */
  UpdateTag updateTags();

  /**
   * begin a CopyTag transaction.
   *
   * <p>
   * This transaction will copy all table versions from a given tag onto this tag
   * </p>
   */
  CopyTag copyTags();

  /**
   * Determines if a Tag is valid.
   *
   * <p>
   *   Validity is defined as a real tag that exists in the backend system
   *   and not a default or null tag
   * </p>
   */
  boolean isValid();

}
