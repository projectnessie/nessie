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
package org.projectnessie.versioned.storage.cache;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.function.Consumer;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.objtypes.CustomObjType;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypeBundle;

public class CacheTestObjTypeBundle implements ObjTypeBundle {

  @Override
  public void register(Consumer<ObjType> registrar) {
    registrar.accept(DefaultCachingObj.TYPE);
    registrar.accept(NonCachingObj.TYPE);
    registrar.accept(DynamicCachingObj.TYPE);
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableDefaultCachingObj.class)
  @JsonDeserialize(as = ImmutableDefaultCachingObj.class)
  interface DefaultCachingObj extends Obj {
    ObjType TYPE = CustomObjType.customObjType("default-caching", "cd", DefaultCachingObj.class);

    @Override
    default ObjType type() {
      return TYPE;
    }

    String value();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableNonCachingObj.class)
  @JsonDeserialize(as = ImmutableNonCachingObj.class)
  interface NonCachingObj extends Obj {
    ObjType TYPE = CustomObjType.uncachedObjType("non-caching", "cnc", NonCachingObj.class);

    @Override
    default ObjType type() {
      return TYPE;
    }

    String value();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableDynamicCachingObj.class)
  @JsonDeserialize(as = ImmutableDynamicCachingObj.class)
  interface DynamicCachingObj extends Obj {
    ObjType TYPE =
        CustomObjType.dynamicCaching(
            "dynamic-caching",
            "cdc",
            DynamicCachingObj.class,
            (obj, currentTimeMicros) -> obj.thatExpireTimestamp() + currentTimeMicros);

    @Override
    default ObjType type() {
      return TYPE;
    }

    long thatExpireTimestamp();
  }
}
