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
package org.projectnessie.catalog.service.objtypes;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.customObjType;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@NessieImmutable
@JsonSerialize(as = ImmutableLakehouseConfigObj.class)
@JsonDeserialize(as = ImmutableLakehouseConfigObj.class)
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface LakehouseConfigObj extends UpdateableObj {
  ObjType OBJ_TYPE = customObjType("lakehouse-config", "lh-cfg", LakehouseConfigObj.class);

  ObjId OBJ_ID = ObjId.objIdFromByteArray("lakehouse-config".getBytes(UTF_8));

  static ImmutableLakehouseConfigObj.Builder builder() {
    return ImmutableLakehouseConfigObj.builder();
  }

  @Override
  @Value.Default
  default ObjId id() {
    return OBJ_ID;
  }

  @Override
  @Value.Default
  default ObjType type() {
    return OBJ_TYPE;
  }

  LakehouseConfig lakehouseConfig();
}
