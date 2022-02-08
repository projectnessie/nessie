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
package org.projectnessie.versioned.testworker;

/** Base content interface for {@link org.projectnessie.versioned.testworker.SimpleStoreWorker}. */
public interface BaseContent {

  enum Type {
    /** Content type with on-reference state. */
    ON_REF_ONLY,
    /** Content type with on-reference state and mandatory global state. */
    WITH_GLOBAL_STATE
  }

  /** Content-id. */
  String getId();
}
