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
package org.projectnessie.events.api;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/** Event that is emitted when a reference is created, updated or deleted. */
public interface ReferenceEvent extends Event {

  /** The name of the reference, e.g. "branch1". */
  String getReferenceName();

  /** The full name of the reference, e.g. "/refs/heads/branch1". */
  String getFullReferenceName();

  /** The type of the reference. */
  ReferenceType getReferenceType();

  interface Builder<B extends Builder<B, E>, E extends ReferenceEvent> extends Event.Builder<B, E> {

    @CanIgnoreReturnValue
    B referenceName(String referenceName);

    @CanIgnoreReturnValue
    B fullReferenceName(String fullReferenceName);

    @CanIgnoreReturnValue
    B referenceType(ReferenceType referenceType);
  }
}
