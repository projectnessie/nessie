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
package org.projectnessie.versioned.persist.adapter;

import java.util.Map;
import org.immutables.value.Value;

/** Keeps track of the logical state of a Nessie repository. */
@Value.Immutable
public interface RepoDescription {
  RepoDescription DEFAULT = builder().repoVersion(0).build();

  /** A logical version number describing the logical data model. */
  int getRepoVersion();

  /** Map of properties for a Nessie repository. */
  Map<String, String> getProperties();

  static ImmutableRepoDescription.Builder builder() {
    return ImmutableRepoDescription.builder();
  }
}
