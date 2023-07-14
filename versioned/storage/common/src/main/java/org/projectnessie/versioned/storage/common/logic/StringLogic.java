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
package org.projectnessie.versioned.storage.common.logic;

import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Provides and encapsulates all logic around string data compression and diff creation and
 * re-assembly of diffs to complete values.
 */
public interface StringLogic {
  StringValue fetchString(ObjId stringObjId) throws ObjNotFoundException;

  StringValue fetchString(StringObj stringObj) throws ObjNotFoundException;

  StringObj updateString(StringValue previousValue, String contentType, byte[] stringValueUtf8)
      throws ObjNotFoundException;

  StringObj updateString(StringValue previousValue, String contentType, String stringValue)
      throws ObjNotFoundException;

  /**
   * Describes a string value and allows retrieval of complete string values. Implementations can
   * lazily decompress data and/or re-assemble values that consist of diffs.
   */
  interface StringValue {
    String contentType();

    String completeValue() throws ObjNotFoundException;

    ObjId objId();
  }
}
