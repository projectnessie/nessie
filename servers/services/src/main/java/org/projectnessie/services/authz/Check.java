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
package org.projectnessie.services.authz;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.NamedRef;

@Value.Immutable
public interface Check {
  CheckType name();

  @Nullable
  NamedRef ref();

  @Nullable
  ContentKey key();

  @Nullable
  String contentId();

  static ImmutableCheck.Builder builder(CheckType type) {
    return ImmutableCheck.builder().name(type);
  }

  enum CheckType {
    VIEW_REFERENCE(true, false),
    CREATE_REFERENCE(true, false),
    ASSIGN_REFERENCE_TO_HASH(true, false),
    DELETE_REFERENCE(true, false),
    READ_ENTRIES(true, false),
    LIST_COMMIT_LOG(true, false),
    COMMIT_CHANGE_AGAINST_REFERENCE(true, false),
    READ_ENTITY_VALUE(true, true),
    UPDATE_ENTITY(true, true),
    DELETE_ENTITY(true, true),
    VIEW_REFLOG(false, false);

    private final boolean ref;
    private final boolean content;

    CheckType(boolean ref, boolean content) {
      this.ref = ref;
      this.content = content;
    }

    public boolean isRef() {
      return ref;
    }

    public boolean isContent() {
      return content;
    }
  }
}
