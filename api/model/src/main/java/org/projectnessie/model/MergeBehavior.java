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
package org.projectnessie.model;

public enum MergeBehavior {
  /** Keys with this merge mode will be merged, conflict detection takes place. */
  NORMAL,
  /** Keys with this merge mode will be merged unconditionally, no conflict detection. */
  FORCE,
  /** Keys with this merge mode will not be merged. */
  DROP,
  /** Apply the {@link Content} value provided via {@link MergeKeyBehavior}. */
  APPLY
}
