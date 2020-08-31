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

import java.util.HashMap;
import java.util.Map;

import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.google.common.collect.Maps;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Generic class for reading a reference.
 */
interface InternalRef extends HasId {

  static final String TYPE = "type";

  public static enum Type {
    BRANCH("b"),
    TAG("t"),
    HASH(null),
    UNKNOWN(null);

    private final AttributeValue value;

    Type(String identifier) {
      this.value = AttributeValue.builder().s(identifier).build();
    }

    public ExpressionFunction typeVerification() {
      return ExpressionFunction.equals(ExpressionPath.builder(TYPE).build(), toAttributeValue());
    }

    public AttributeValue toAttributeValue() {
      if (this == HASH) {
        throw new IllegalStateException("You should not try to retrieve the identifier for a hash "
            + "type since they are not saveable as searchable refs.");
      }
      return value;
    }

    public static Type getType(String identifier) {
      if (identifier.equals("b")) {
        return BRANCH;
      } else if (identifier.equals("t")) {
        return TAG;
      } else {
        throw new IllegalArgumentException(String.format("Unknown identifier name [%s].", identifier));
      }
    }
  }

  Type getType();

  default InternalBranch getBranch() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a branch.", this.getClass().getName()));
  }

  default InternalTag getTag() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a tag.", this.getClass().getName()));
  }

  default Id getHash() {
    throw new IllegalArgumentException(String.format("%s cannot be treated as a hash.", this.getClass().getName()));
  }

  Id getId();

  static final SimpleSchema<InternalRef> SCHEMA = new SimpleSchema<InternalRef>(InternalRef.class) {

    @Override
    public InternalRef deserialize(Map<String, AttributeValue> attributeMap) {
      Type type = Type.getType(attributeMap.get(TYPE).s());
      Map<String, AttributeValue> filtered = Maps.filterEntries(attributeMap, e -> !e.getKey().equals(TYPE));
      switch (type) {
        case BRANCH: return InternalBranch.SCHEMA.mapToItem(filtered);
        case TAG: return InternalTag.SCHEMA.mapToItem(filtered);
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public Map<String, AttributeValue> itemToMap(InternalRef item, boolean ignoreNulls) {
      Map<String, AttributeValue> map = new HashMap<>();
      map.put(TYPE, item.getType().toAttributeValue());

      switch (item.getType()) {
        case BRANCH:
          map.putAll(InternalBranch.SCHEMA.itemToMap(item.getBranch(), ignoreNulls));
          break;
        case TAG:
          map.putAll(InternalTag.SCHEMA.itemToMap(item.getTag(), ignoreNulls));
          break;
        default:
          throw new UnsupportedOperationException();
      }

      return map;
    }

  };

}
