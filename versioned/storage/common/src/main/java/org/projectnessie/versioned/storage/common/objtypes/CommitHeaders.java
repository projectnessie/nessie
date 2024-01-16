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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import org.projectnessie.versioned.storage.common.persist.Hashable;
import org.projectnessie.versioned.storage.common.persist.ObjIdHasher;

public interface CommitHeaders extends Hashable {

  CommitHeaders EMPTY_COMMIT_HEADERS = newCommitHeaders().build();

  static String capitalizeName(String name) {
    StringBuilder sb = new StringBuilder();
    boolean word = true;
    int l = name.length();
    for (int i = 0; i < l; i++) {
      char c = name.charAt(i);
      boolean letter = Character.isLetter(c);
      if (word) {
        if (letter) {
          word = false;
          c = Character.toUpperCase(c);
        }
      } else {
        if (!letter) {
          word = true;
        } else {
          c = Character.toLowerCase(c);
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  String getFirst(String name);

  List<String> getAll(String name);

  Set<String> keySet();

  default Builder toBuilder() {
    return newCommitHeaders().from(this);
  }

  static Builder newCommitHeaders() {
    return CommitHeadersImpl.builder();
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder add(String name, String value);

    @CanIgnoreReturnValue
    Builder from(CommitHeaders headers);

    @CanIgnoreReturnValue
    default Builder add(String name, OffsetDateTime offsetDateTime) {
      return add(name, offsetDateTime.toString());
    }

    CommitHeaders build();
  }

  @Override
  default void hash(ObjIdHasher hasher) {
    for (String header : keySet()) {
      hasher.hash(header);
      List<String> values = getAll(header);
      for (String value : values) {
        hasher.hash(value);
      }
    }
  }
}
