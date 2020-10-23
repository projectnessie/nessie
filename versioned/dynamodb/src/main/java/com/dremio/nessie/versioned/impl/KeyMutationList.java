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

import java.util.List;
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

}
