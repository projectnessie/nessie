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
package org.projectnessie.model;

public enum CommitConsistency {
  // NOTE: the order of the values represents the "health" of a commit, best to worst.

  /** Consistency was not checked. */
  NOT_CHECKED,
  /** The commit object, its index information and all reachable content is present. */
  COMMIT_CONSISTENT,
  /**
   * The commit object is present and its index is accessible, but some content reachable from the
   * commit is not present.
   */
  COMMIT_CONTENT_INCONSISTENT,
  /**
   * The commit is inconsistent in a way that makes it impossible to access the commit, for example
   * if the commit object itself or its index information is missing.
   */
  COMMIT_INCONSISTENT
}
