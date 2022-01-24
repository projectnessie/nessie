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

import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * API for Adding View Column comments
 *
 * <p>When committing, these changes will be applied to the current view metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdateComment extends PendingUpdate<Schema> {
  /**
   * Update the comment of a column in the schema.
   *
   * <p>The name is used to find the column to update using {@link Schema#findField(String)}.
   *
   * <p>
   *
   * @param name name of the column whose comment is being updated
   * @param newDoc replacement comment string for the column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema.
   */
  UpdateComment updateColumnDoc(String name, String newDoc);
}
