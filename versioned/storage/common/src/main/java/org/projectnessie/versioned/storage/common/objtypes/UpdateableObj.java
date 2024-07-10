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
package org.projectnessie.versioned.storage.common.objtypes;

import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_VERS_KEY;
import static org.projectnessie.versioned.storage.common.objtypes.GenericObj.VERSION_TOKEN_ATTRIBUTE;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Optional;
import org.projectnessie.versioned.storage.common.persist.Obj;

public interface UpdateableObj extends Obj {

  static Optional<String> extractVersionToken(Obj obj) {
    String versionToken = null;
    if (obj instanceof UpdateableObj) {
      versionToken = ((UpdateableObj) obj).versionToken();
    } else if (obj instanceof GenericObj) {
      GenericObj genericObj = (GenericObj) obj;
      versionToken = (String) genericObj.attributes().get(VERSION_TOKEN_ATTRIBUTE);
    }
    return Optional.ofNullable(versionToken);
  }

  @JsonIgnore
  @JacksonInject(OBJ_VERS_KEY)
  String versionToken();
}
