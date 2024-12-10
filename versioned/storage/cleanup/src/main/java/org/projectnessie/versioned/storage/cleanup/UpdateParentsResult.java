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
package org.projectnessie.versioned.storage.cleanup;

import java.util.Map;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/** Represents results of updating the commit history. */
@NessieImmutable
public interface UpdateParentsResult {

  /** Update failures by commit ID. */
  Map<ObjId, Throwable> failures();

  static ImmutableUpdateParentsResult.Builder builder() {
    return ImmutableUpdateParentsResult.builder();
  }
}
