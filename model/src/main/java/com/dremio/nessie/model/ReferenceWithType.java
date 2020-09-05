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
package com.dremio.nessie.model;

import java.io.IOException;

import org.immutables.value.Value;

import com.dremio.nessie.model.ReferenceWithType.ReferenceWithTypeDeserializer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * This class helps return polymorphic types in TableBranchOperations.
 *
 * <p>
 *   This is not a great way to do polymorphic types in Jackson and it adds extra code to keep up. Ideally we would remove this
 *   and do polymorphic types in a more sophisticated way. Additionally the current REST api endpoints that need this are probably
 *   not the best designed and should be removed.
 *   - Ryan Sept 3 2020
 * </p>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableReferenceWithType.class)
@JsonDeserialize(using = ReferenceWithTypeDeserializer.class)
public abstract class ReferenceWithType<T extends Reference> {

  /**
   * return type of reference being contained.
   */
  public String getType() {
    T ref = getReference();
    if (ref instanceof Tag) {
      return "TAG";
    } else if (ref instanceof Branch) {
      return "BRANCH";
    } else if (ref instanceof Hash) {
      return "HASH";
    }
    throw new UnsupportedOperationException(String.format("Unknown type %s", ref));
  }

  public abstract T getReference();

  public static <T extends Reference> ReferenceWithType<T> of(T obj) {
    return (ReferenceWithType<T>) ImmutableReferenceWithType.builder().reference(obj).build();
  }

  public static class ReferenceWithTypeDeserializer<R extends Reference> extends StdDeserializer<ReferenceWithType<R>> {
    public ReferenceWithTypeDeserializer() {
      this(null);
    }

    public ReferenceWithTypeDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public ReferenceWithType<R> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      JsonNode node = p.getCodec().readTree(p);
      String id = node.get("type").asText();
      final Reference ref;
      if (id.equals("TAG")) {
        ref = p.getCodec().treeToValue(node.get("reference"), Tag.class);
      } else if (id.equals("BRANCH")) {
        ref = p.getCodec().treeToValue(node.get("reference"), Branch.class);
      } else if (id.equals("HASH")) {
        ref = p.getCodec().treeToValue(node.get("reference"), Hash.class);
      } else {
        throw new UnsupportedOperationException(String.format("Unknown type %s", id));
      }
      return (ReferenceWithType<R>) of(ref);
    }
  }
}
