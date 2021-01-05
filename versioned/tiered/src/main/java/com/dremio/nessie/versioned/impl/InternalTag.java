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

import java.util.Map;

import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.tiered.builder.RefConsumer.RefType;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

class InternalTag extends MemoizedId<RefConsumer> implements InternalRef {

  static final String ID = "id";
  static final String NAME = "name";
  static final String COMMIT = "commit";

  private String name;
  private Id commit;

  InternalTag(Id id, String name, Id commit) {
    super(id);
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

  public Map<String, Entity> conditionMap() {
    return ImmutableMap.of(COMMIT, commit.toEntity());
  }

  static final SimpleSchema<InternalTag> SCHEMA = new SimpleSchema<InternalTag>() {


    @Override
    public InternalTag deserialize(Map<String, Entity> attributeMap) {
      return new InternalTag(
          Id.fromEntity(attributeMap.get(ID)),
          attributeMap.get(NAME).getString(),
          Id.fromEntity(attributeMap.get(COMMIT))
          );
    }

    @Override
    public Map<String, Entity> itemToMap(InternalTag item, boolean ignoreNulls) {
      return ImmutableMap.<String, Entity>builder()
          .put(ID, item.getId().toEntity())
          .put(COMMIT, item.commit.toEntity())
          .put(NAME, Entity.ofString(item.name))
          .build();
    }

  };

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
  public RefConsumer applyToConsumer(RefConsumer consumer) {
    return consumer.id(getId())
        .name(name)
        .type(RefType.TAG)
        .commit(commit);
  }

}


