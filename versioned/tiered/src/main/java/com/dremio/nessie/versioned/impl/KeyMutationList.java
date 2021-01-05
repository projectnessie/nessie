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
package com.dremio.nessie.versioned.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value.Immutable;

import com.dremio.nessie.versioned.store.Entity;

@Immutable
abstract class KeyMutationList {

  abstract List<KeyMutation> getMutations();

  static KeyMutationList of(List<KeyMutation> mutations) {
    return ImmutableKeyMutationList.builder().mutations(mutations).build();
  }

  public Entity toEntity() {
    return Entity.ofList(getMutations().stream().map(KeyMutation::toEntity).collect(Collectors.toList()));
  }

  public static KeyMutationList fromEntity(Entity value) {
    return ImmutableKeyMutationList.builder().addAllMutations(
        value.getList().stream().map(KeyMutation::fromEntity).collect(Collectors.toList()))
        .build();
  }

  /**
   * Compare the given {@link KeyMutationList}s but ignores the order of the mutations in both
   * lists.
   */
  public static boolean equalsIgnoreOrder(KeyMutationList l1, KeyMutationList l2) {
    if (l1 != null && l2 == null) {
      return false;
    }
    if (l1 == null && l2 != null) {
      return false;
    }
    if (l1 == null) {
      return true;
    }
    return l1.equalsIgnoreOrder(l2);
  }

  /**
   * Compare the given {@link KeyMutationList} with this one but ignores the order of the
   * mutations in both lists.
   */
  public boolean equalsIgnoreOrder(KeyMutationList o) {
    if (o == null) {
      return false;
    }

    List<KeyMutation> mm = getMutations();
    List<KeyMutation> om = o.getMutations();
    if (mm.size() != om.size()) {
      return false;
    }

    Set<KeyMutation> s = new HashSet<>(mm);
    for (KeyMutation km : om) {
      if (!s.contains(km)) {
        return false;
      }
    }

    return true;
  }
}
