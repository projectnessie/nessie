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
package org.projectnessie.versioned;

import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Informational object to tell a client about the Nessie repository.
 *
 * <p>All values are optional and may or may not be present, depending on the actual server version.
 */
@Value.Immutable
public interface RepositoryInformation {
  @Nullable
  String getDefaultBranch();

  @Nullable
  String getNoAncestorHash();

  @Nullable
  Instant getRepositoryCreationTimestamp();

  @Nullable
  Instant getOldestPossibleCommitTimestamp();

  Map<String, String> getAdditionalProperties();
}
