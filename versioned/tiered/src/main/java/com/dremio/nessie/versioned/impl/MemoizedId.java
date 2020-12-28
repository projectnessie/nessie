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

import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;

abstract class MemoizedId implements HasId {

  // unchanging but only generated once needed.
  private Id id;

  MemoizedId() {
    this.id = null;
  }

  MemoizedId(Id id) {
    this.id = id;
  }

  abstract Id generateId();

  @Override
  public final Id getId() {
    if (id == null) {
      id = generateId();
    }
    return id;
  }

  void ensureConsistentId() {
    if (id != null) {
      assert id.equals(generateId());
    }
  }
}
