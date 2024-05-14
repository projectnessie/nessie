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

import static java.util.Arrays.asList;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/** A required {@link Obj} could not be found. */
public class ObjNotFoundException extends Exception {
  private final ObjId[] objIds;

  public ObjNotFoundException(@Nonnull ObjId objId) {
    super("Object with ID " + objId + " not found");
    this.objIds = new ObjId[] {objId};
  }

  public ObjNotFoundException(@Nonnull List<ObjId> objIds) {
    super(
        objIds.size() == 1
            ? "Object with ID " + objIds.get(0) + " not found"
            : "Objects with IDs "
                + objIds.stream().map(ObjId::toString).collect(Collectors.joining(","))
                + " not found");
    this.objIds = objIds.toArray(new ObjId[0]);
  }

  public List<ObjId> objIds() {
    return asList(objIds);
  }
}
