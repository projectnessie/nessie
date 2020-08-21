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

import com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class InternalTag extends MemoizedId {

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

  public Map<String, AttributeValue> conditionMap() {
    return ImmutableMap.of(COMMIT, commit.toAttributeValue());
  }

  static final TableSchema<InternalTag> SCHEMA = new SimpleSchema<InternalTag>(InternalTag.class) {


    @Override
    public InternalTag deserialize(Map<String, AttributeValue> attributeMap) {
      return new InternalTag(
          Id.fromAttributeValue(attributeMap.get(ID)),
          attributeMap.get(NAME).s(),
          Id.fromAttributeValue(attributeMap.get(COMMIT))
          );
    }

    @Override
    public Map<String, AttributeValue> itemToMap(InternalTag item, boolean ignoreNulls) {
      return ImmutableMap.<String, AttributeValue>builder()
          .put(ID, item.getId().toAttributeValue())
          .put(COMMIT, item.commit.toAttributeValue())
          .put(NAME, AttributeValue.builder().s(item.name).build())
          .build();
    }

  };

  public InternalRef asRef() {
    return InternalRef.of(this);
  }

}


