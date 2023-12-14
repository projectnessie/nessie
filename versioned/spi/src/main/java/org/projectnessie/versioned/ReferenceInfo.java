/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned;

import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface ReferenceInfo<METADATA> {
  NamedRef getNamedRef();

  Hash getHash();

  @Value.Default
  default long getCommitSeq() {
    return 0L;
  }

  @Nullable
  Hash getCommonAncestor();

  ReferenceInfo<METADATA> withCommonAncestor(@Nullable Hash value);

  @Nullable
  CommitsAheadBehind getAheadBehind();

  ReferenceInfo<METADATA> withAheadBehind(@Nullable CommitsAheadBehind value);

  @Nullable
  METADATA getHeadCommitMeta();

  ReferenceInfo<METADATA> withHeadCommitMeta(@Nullable METADATA value);

  List<Hash> getParentHashes();

  @SuppressWarnings({"rawtypes", "unchecked"})
  default <UPDATED_METADATA> ReferenceInfo<UPDATED_METADATA> withUpdatedCommitMeta(
      UPDATED_METADATA commitMeta) {
    ReferenceInfo updated = this;
    return updated.withHeadCommitMeta(commitMeta);
  }

  static <METADATA> ImmutableReferenceInfo.Builder<METADATA> builder() {
    return ImmutableReferenceInfo.builder();
  }

  static <METADATA> ReferenceInfo<METADATA> of(Hash hash, NamedRef namedRef) {
    return ReferenceInfo.<METADATA>builder().namedRef(namedRef).hash(hash).build();
  }

  @Value.Immutable
  interface CommitsAheadBehind {

    int getBehind();

    int getAhead();

    static CommitsAheadBehind of(int ahead, int behind) {
      return ImmutableCommitsAheadBehind.builder().ahead(ahead).behind(behind).build();
    }
  }
}
