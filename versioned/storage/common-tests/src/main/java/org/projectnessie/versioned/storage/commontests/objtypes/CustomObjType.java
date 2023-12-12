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

import org.projectnessie.versioned.storage.common.objtypes.JsonObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public enum CustomObjType implements ObjType {

  /** A custom object built from scratch. */
  SIMPLE("smp", SimpleTestObj.class),

  /** A custom object extending {@link JsonObj}. */
  JSON("jsn", JsonTestObj.class);

  private final String shortName;
  private final Class<? extends Obj> targetClass;

  CustomObjType(String shortName, Class<? extends Obj> targetClass) {
    this.shortName = shortName;
    this.targetClass = targetClass;
  }

  @Override
  public String shortName() {
    return shortName;
  }

  @Override
  public Class<? extends Obj> targetClass() {
    return targetClass;
  }
}
