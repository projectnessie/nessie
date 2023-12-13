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
package org.projectnessie.versioned.storage.common.exceptions;

import jakarta.annotation.Nullable;
import org.projectnessie.versioned.storage.common.persist.Reference;

public class RefAlreadyExistsException extends RefException {

  /**
   * Indicates that an attempt to create a reference failed, because the same name already exists.
   *
   * @param existing the already existing reference, might be {@code null}, if a race with a
   *     concurrent reference-delete-operation happened. This parameter should <em>always</em>
   *     reflect the state fetched from the database and <em>never</em> the one of the
   *     create-attempt.
   */
  public RefAlreadyExistsException(@Nullable Reference existing) {
    super(
        existing,
        existing != null
            ? "Reference " + existing.name() + " already exists"
            : "Reference already exists");
  }
}
