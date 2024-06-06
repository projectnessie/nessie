/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.persist;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

public interface Backend extends AutoCloseable {

  /**
   * Set up the schema for the backend and create the necessary tables / collections / column
   * families.
   *
   * @return any optional information about the schema setup, such as the database name; to be
   *     displayed when the application starts.
   */
  Optional<String> setupSchema();

  @Nonnull
  PersistFactory createFactory();

  void eraseRepositories(Set<String> repositoryIds);
}
