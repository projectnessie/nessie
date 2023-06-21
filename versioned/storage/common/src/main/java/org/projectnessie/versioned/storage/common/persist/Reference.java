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
package org.projectnessie.versioned.storage.common.persist;

import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.logic.InternalRef;

/**
 * Reference is a generic named pointer.
 *
 * <ul>
 *   <li>Branches: reference names for branches start with {@code refs/heads/} and points to {@link
 *       ObjType#COMMIT}.
 *   <li>Tags: reference names for branches start with {@code refs/tags/} and points to {@link
 *       ObjType#TAG} or {@link ObjType#COMMIT}.
 *   <li>Internal references, not publicly exposed, see {@link InternalRef}.
 * </ul>
 */
@Value.Immutable
public interface Reference {

  String INTERNAL_PREFIX = "int/";

  @Value.Parameter(order = 2)
  String name();

  @Value.Parameter(order = 3)
  ObjId pointer();

  @Value.Parameter(order = 4)
  boolean deleted();

  default Reference forNewPointer(ObjId newPointer) {
    return ImmutableReference.builder().from(this).deleted(false).pointer(newPointer).build();
  }

  Reference withDeleted(boolean deleted);

  static Reference reference(String name, ObjId pointer, boolean deleted) {
    return ImmutableReference.of(name, pointer, deleted);
  }

  @Value.NonAttribute
  default boolean isInternal() {
    return name().startsWith(INTERNAL_PREFIX);
  }

  static boolean isInternalReferenceName(String name) {
    return name.startsWith(INTERNAL_PREFIX);
  }
}
