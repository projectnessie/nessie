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
package org.projectnessie.versioned.storage.mongodb.serializers;

import org.bson.Document;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public interface ObjSerializer<O extends Obj> {

  /**
   * The name of the document's top-level field that will contain the serialized object. Note that
   * this is generally equal to {@link ObjType#shortName()}, but not always, for historical reasons.
   */
  String fieldName();

  void objToDoc(O obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize)
      throws ObjTooLargeException;

  O docToObj(ObjId id, ObjType type, long referenced, Document doc, String versionToken);
}
