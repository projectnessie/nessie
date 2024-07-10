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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.CustomObjType;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
@JsonSerialize(as = ImmutableVersionedTestObj2.class)
@JsonDeserialize(as = ImmutableVersionedTestObj2.class)
public interface VersionedTestObj2 extends UpdateableObj {

  ObjType TYPE = CustomObjType.customObjType("versioned2", "vers2", VersionedTestObj2.class);

  @Override
  default ObjType type() {
    return TYPE;
  }

  static ImmutableVersionedTestObj2.Builder builder() {
    return ImmutableVersionedTestObj2.builder();
  }

  String otherValue();
}
