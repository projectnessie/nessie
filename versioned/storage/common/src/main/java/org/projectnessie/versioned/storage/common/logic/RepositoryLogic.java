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
package org.projectnessie.versioned.storage.common.logic;

import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;

/** Logic to setup/initialize a Nessie repository. */
public interface RepositoryLogic {

  void initialize(@Nonnull @jakarta.annotation.Nonnull String defaultBranchName);

  void initialize(
      @Nonnull @jakarta.annotation.Nonnull String defaultBranchName,
      boolean createDefaultBranch,
      Consumer<RepositoryDescription.Builder> repositoryDescription);

  @Nullable
  @jakarta.annotation.Nullable
  RepositoryDescription fetchRepositoryDescription();

  RepositoryDescription updateRepositoryDescription(RepositoryDescription repositoryDescription)
      throws RetryTimeoutException;

  boolean repositoryExists();
}
