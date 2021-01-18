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

import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Objects;

class InternalTag extends InternalRef {

  static final String COMMIT = "commit";

  private final String name;
  private final Id commit;

  InternalTag(Id id, String name, Id commit, Long dt) {
    super(id, dt);
    this.name = name;
    this.commit = commit;
  }

  @Override
  Id generateId() {
    return Id.build(name);
  }

  public String getName() {
    return name;
  }

  public Id getCommit() {
    return commit;
  }

  @Override
  public Type getType() {
    return Type.TAG;
  }

  @Override
  public InternalTag getTag() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InternalTag that = (InternalTag) o;
    return Objects.equal(name, that.name) && Objects
        .equal(commit, that.commit);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, commit);
  }

  @Override
  Ref applyToConsumer(Ref consumer) {
    return super.applyToConsumer(consumer)
        .name(name)
        .tag()
        .commit(commit);
  }

}


