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
package org.projectnessie.versioned.storage.common.persist;

import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;

public interface ValidatingPersist extends Persist {

  default void verifySoftRestrictions(Obj obj) throws ObjTooLargeException {
    if (obj instanceof CommitObj) {
      CommitObj c = (CommitObj) obj;
      ByteString serializedIndex = c.incrementalIndex();
      if (serializedIndex.size()
          > Math.min(config().maxIncrementalIndexSize(), hardObjectSizeLimit() / 2)) {
        throw new ObjTooLargeException(
            serializedIndex.size(),
            Math.min(config().maxIncrementalIndexSize(), hardObjectSizeLimit() / 2));
      }
    } else if (obj instanceof IndexObj) {
      IndexObj s = (IndexObj) obj;
      ByteString index = s.index();
      if (index.size() > Math.min(config().maxSerializedIndexSize(), hardObjectSizeLimit() / 2)) {
        throw new ObjTooLargeException(
            index.size(), Math.min(config().maxSerializedIndexSize(), hardObjectSizeLimit() / 2));
      }
    }
  }
}
