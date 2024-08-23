/*
 * Copyright (C) 2022 Dremio
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

import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX_SEGMENTS;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface IndexSegmentsObj extends Obj {

  @Override
  default ObjType type() {
    return INDEX_SEGMENTS;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  ObjId id();

  @Override
  @Value.Parameter(order = 1)
  @Value.Auxiliary
  long referenced();

  @Value.Parameter(order = 2)
  List<IndexStripe> stripes();

  @Nonnull
  static IndexSegmentsObj indexSegments(
      @Nullable ObjId id, long referenced, @Nonnull List<IndexStripe> stripes) {
    return ImmutableIndexSegmentsObj.of(id, referenced, stripes);
  }

  @Nonnull
  static IndexSegmentsObj indexSegments(@Nonnull List<IndexStripe> stripes) {
    return indexSegments(
        objIdHasher(INDEX_SEGMENTS).hashCollection(stripes).generate(), 0L, stripes);
  }
}
