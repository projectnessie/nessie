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

import static org.projectnessie.versioned.storage.common.objtypes.Hashes.indexSegmentsHash;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface IndexSegmentsObj extends Obj {

  @Override
  default ObjType type() {
    return StandardObjType.INDEX_SEGMENTS;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  @jakarta.annotation.Nullable
  ObjId id();

  @Value.Parameter(order = 2)
  List<IndexStripe> stripes();

  @Nonnull
  @jakarta.annotation.Nonnull
  static IndexSegmentsObj indexSegments(
      @Nullable @jakarta.annotation.Nullable ObjId id,
      @Nonnull @jakarta.annotation.Nonnull List<IndexStripe> stripes) {
    return ImmutableIndexSegmentsObj.of(id, stripes);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  static IndexSegmentsObj indexSegments(
      @Nonnull @jakarta.annotation.Nonnull List<IndexStripe> stripes) {
    return indexSegments(indexSegmentsHash(stripes), stripes);
  }
}
