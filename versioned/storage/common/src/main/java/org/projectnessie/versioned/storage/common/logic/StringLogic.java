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

import jakarta.annotation.Nullable;
import java.util.function.Consumer;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CreateCommit.Builder;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

/**
 * Provides and encapsulates all logic around string data compression and diff creation and
 * re-assembly of diffs to complete values.
 */
public interface StringLogic {
  StringValue fetchString(ObjId stringObjId) throws ObjNotFoundException;

  StringValue fetchString(StringObj stringObj);

  StringObj updateString(StringValue previousValue, String contentType, byte[] stringValueUtf8);

  StringObj updateString(StringValue previousValue, String contentType, String stringValue);

  /**
   * Updates a string value stored as a content on a reference.
   *
   * @param reference The reference to update.
   * @param storeKey The store key to use for storing the string value.
   * @param commitEnhancer A consumer that can be used to enhance the commit that will be created.
   * @param contentType The content type of the string value.
   * @param stringValueUtf8 The string value as UTF-8 encoded byte array.
   * @return The previously stored string value, if any, or {@code null} if no string value was
   *     stored on the reference.
   */
  @Nullable
  StringValue updateStringOnRef(
      Reference reference,
      StoreKey storeKey,
      Consumer<Builder> commitEnhancer,
      String contentType,
      byte[] stringValueUtf8)
      throws ObjNotFoundException,
          CommitConflictException,
          RefNotFoundException,
          RefConditionFailedException;

  /**
   * Describes a string value and allows retrieval of complete string values. Implementations can
   * lazily decompress data and/or re-assemble values that consist of diffs.
   */
  interface StringValue {
    String contentType();

    String completeValue();

    ObjId objId();
  }
}
