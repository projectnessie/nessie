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
package org.projectnessie.catalog.service.impl;

import static org.projectnessie.catalog.model.id.NessieId.emptyNessieId;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromByteAccessor;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromLongs;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteAccessor;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromLongs;
import static org.projectnessie.versioned.storage.common.persist.ObjId.zeroLengthObjId;

import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class Util {
  private Util() {}

  static ObjId nessieIdToObjId(NessieId id) {
    switch (id.size()) {
      case 32:
        return objIdFromLongs(id.longAt(0), id.longAt(1), id.longAt(2), id.longAt(3));
      case 0:
        return zeroLengthObjId();
      default:
        return objIdFromByteAccessor(id.size(), id::byteAt);
    }
  }

  static NessieId objIdToNessieId(ObjId id) {
    switch (id.size()) {
      case 32:
        return nessieIdFromLongs(id.longAt(0), id.longAt(1), id.longAt(2), id.longAt(3));
      case 0:
        return emptyNessieId();
      default:
        return nessieIdFromByteAccessor(id.size(), id::byteAt);
    }
  }
}
