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

import static java.util.Collections.emptyList;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.InternalRef;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;

/**
 * Reference is a generic named pointer.
 *
 * <ul>
 *   <li>Branches: reference names for branches start with {@code refs/heads/} and points to {@link
 *       StandardObjType#COMMIT}.
 *   <li>Tags: reference names for branches start with {@code refs/tags/} and points to {@link
 *       StandardObjType#TAG} or {@link StandardObjType#COMMIT}.
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

  @Value.Parameter(order = 5)
  long createdAtMicros();

  @Value.Parameter(order = 6)
  @Nullable
  ObjId extendedInfoObj();

  // Not included in .equals/.hashCode to avoid having separate equals() implementation that does
  // not consider this attribute (esp. for in-memory and rocks-db backends).
  @Value.Auxiliary
  @Value.Parameter(order = 7)
  List<PreviousPointer> previousPointers();

  default Reference forNewPointer(ObjId newPointer, StoreConfig config) {
    List<PreviousPointer> previous = new ArrayList<>();

    long now = config.currentTimeMicros();
    int sizeLimit = config.referencePreviousHeadCount();
    long timeLimit = now - TimeUnit.SECONDS.toMicros(config.referencePreviousHeadTimeSpanSeconds());

    if (!newPointer.equals(pointer())) {
      previous.add(PreviousPointer.previousPointer(pointer(), now));
    }
    for (PreviousPointer previousPointer : previousPointers()) {
      if (previous.size() == sizeLimit || (previousPointer.timestamp() - timeLimit < 0L)) {
        break;
      }
      previous.add(previousPointer);
    }

    return ImmutableReference.builder()
        .from(this)
        .deleted(false)
        .pointer(newPointer)
        .previousPointers(previous)
        .build();
  }

  Reference withDeleted(boolean deleted);

  static Reference reference(
      String name, ObjId pointer, boolean deleted, long createdAtMicros, ObjId extendedInfoObj) {
    return reference(name, pointer, deleted, createdAtMicros, extendedInfoObj, emptyList());
  }

  static Reference reference(
      String name,
      ObjId pointer,
      boolean deleted,
      long createdAtMicros,
      ObjId extendedInfoObj,
      List<PreviousPointer> previousPointers) {
    return ImmutableReference.of(
        name, pointer, deleted, createdAtMicros, extendedInfoObj, previousPointers);
  }

  @Value.NonAttribute
  default boolean isInternal() {
    return name().startsWith(INTERNAL_PREFIX);
  }

  static boolean isInternalReferenceName(String name) {
    return name.startsWith(INTERNAL_PREFIX);
  }

  @Value.Immutable
  interface PreviousPointer {
    @Value.Parameter(order = 1)
    ObjId pointer();

    @Value.Parameter(order = 2)
    long timestamp();

    static PreviousPointer previousPointer(ObjId pointer, long timestamp) {
      return ImmutablePreviousPointer.of(pointer, timestamp);
    }
  }
}
