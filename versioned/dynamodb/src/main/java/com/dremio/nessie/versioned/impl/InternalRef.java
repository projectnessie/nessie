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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class InternalRef implements HasId {
  public static enum Type {
    BRANCH("b"),
    TAG("t"),
    HASH(null),
    UNKNOWN(null);

    private final String identifier;
    private final AttributeValue value;

    Type(String identifier) {
      this.identifier = identifier;
      this.value = AttributeValue.builder().s(identifier).build();
    }

    public ExpressionFunction typeVerification() {
      return ExpressionFunction.equals(ExpressionPath.builder(TYPE).build(), toAttributeValue());
    }

    public String identifier() {
      if(this==HASH) {
        throw new IllegalStateException("You should not try to retrieve the identifier for a hash type since they are not saveable as searchable refs.");
      }
      return identifier;
    }

    public AttributeValue toAttributeValue() {
      return value;
    }

    public static Type getType(String identifier) {
      if(identifier.equals("b")) {
        return BRANCH;
      } else if (identifier.equals("t")) {
        return TAG;
      } else {
        throw new IllegalArgumentException(String.format("Unknown identifier name [%s].", identifier));
      }
    }
  }


  static final String TYPE = "type";

  private final Type type;
  private final InternalBranch branch;
  private final InternalTag tag;
  private final Id hash;

  private InternalRef(Type type, InternalBranch branch, InternalTag tag, Id hash) {
    super();
    this.type = type;
    this.branch = branch;
    this.tag = tag;
    this.hash = hash;
  }

  public Type getType() {
    return type;
  }

  public InternalBranch getBranch() {
    Preconditions.checkArgument(type == Type.BRANCH);
    return branch;
  }

  public InternalTag getTag() {
    Preconditions.checkArgument(type == Type.TAG);
    return tag;
  }

  public Id getHash() {
    Preconditions.checkArgument(type == Type.HASH);
    return hash;
  }

  public Id getId() {
    switch(type) {
    case BRANCH:
      return branch.getId();
    case HASH:
      return Id.of(hash.getValue().asReadOnlyByteBuffer());
    case TAG:
      return tag.getCommit();
    case UNKNOWN:
    default:
      throw new IllegalStateException();
    }
  }

  static InternalRef of(InternalBranch branch) {
    return new InternalRef(Type.BRANCH, branch, null, null);
  }

  static InternalRef of(Id id) {
    return new InternalRef(Type.HASH, null, null, id);
  }

  static InternalRef of(InternalTag tag) {
    return new InternalRef(Type.TAG, null, tag, null);
  }

  static final TableSchema<InternalRef> SCHEMA = new SimpleSchema<InternalRef>(InternalRef.class) {

    @Override
    public InternalRef deserialize(Map<String, AttributeValue> attributeMap) {
      Type type = Type.getType(attributeMap.get(TYPE).s());
      Map<String, AttributeValue> filtered = Maps.filterEntries(attributeMap, e -> !e.getKey().equals(TYPE));
      switch(type) {
      case BRANCH: return new InternalRef(type, InternalBranch.SCHEMA.mapToItem(filtered), null, null);
      case TAG: return new InternalRef(type, null, InternalTag.SCHEMA.mapToItem(filtered), null);
      default:
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public Map<String, AttributeValue> itemToMap(InternalRef item, boolean ignoreNulls) {
      Map<String, AttributeValue> map = new HashMap<>();
      map.put(TYPE, item.type.toAttributeValue());

      switch(item.type) {
      case BRANCH:
        map.putAll(InternalBranch.SCHEMA.itemToMap(item.branch, ignoreNulls));
        break;
      case TAG:
        map.putAll(InternalTag.SCHEMA.itemToMap(item.tag, ignoreNulls));
        break;
      default:
        throw new UnsupportedOperationException();
      }

      return map;
    }

  };

}
