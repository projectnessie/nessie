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
package org.projectnessie.versioned.storage.commontests.objtypes;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.JsonObj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
@JsonSerialize(as = ImmutableJsonTestObj.class)
@JsonDeserialize(as = ImmutableJsonTestObj.class)
public interface JsonTestObj extends JsonObj<JsonTestBean> {

  @Override
  default ObjType type() {
    return CustomObjType.JSON;
  }

  @Override
  @JsonUnwrapped
  JsonTestBean model();
}
