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
package org.projectnessie.services.authz;

import org.projectnessie.model.ContentsKey;
import org.projectnessie.versioned.NamedRef;

public interface AccessChecker {

  void canCreateReference(AccessContext context, NamedRef ref);

  void canAssignRefToHash(AccessContext context, NamedRef ref);

  void canDeleteReference(AccessContext context, NamedRef ref);

  void canReadObjectContent(AccessContext context, NamedRef ref);

  void canListObjects(AccessContext context, NamedRef ref);

  void canCommitChangeAgainstReference(AccessContext context, NamedRef ref);

  void canReadEntityValue(AccessContext context, NamedRef ref, ContentsKey key);

  void canUpdateEntity(AccessContext context, NamedRef ref, ContentsKey key);

  void canDeleteEntity(AccessContext context, NamedRef ref, ContentsKey key);
}
