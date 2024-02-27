/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cache;

import org.projectnessie.versioned.storage.common.persist.ObjId;

public interface DistributedCacheInvalidation {
  void removeObj(String repositoryId, ObjId objId);

  void putObj(String repositoryId, ObjId objId, int hash);

  void removeReference(String repositoryId, String refName);

  void putReference(String repositoryId, String refName, int hash);
}