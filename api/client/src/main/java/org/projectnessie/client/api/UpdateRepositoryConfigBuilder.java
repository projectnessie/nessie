/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.api;

import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.UpdateRepositoryConfigResponse;

public interface UpdateRepositoryConfigBuilder {
  /**
   * Sets the repository config to create or update. Only one repository config can be created or
   * updated.
   */
  UpdateRepositoryConfigBuilder repositoryConfig(RepositoryConfig update);

  /**
   * Perform the request to create or update the {@link #repositoryConfig(RepositoryConfig)
   * repository config}.
   */
  UpdateRepositoryConfigResponse update() throws NessieConflictException;
}
