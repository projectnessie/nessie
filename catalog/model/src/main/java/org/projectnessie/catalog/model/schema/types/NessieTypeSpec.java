/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.model.schema.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import java.io.IOException;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.Hashable;

/** Field/column type specification base. */
@JsonTypeIdResolver(NessieTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type")
@JsonSerialize(using = NessieTypeSpec.NessieTypeSpecSerializer.class)
@JsonDeserialize(using = NessieTypeSpec.NessieTypeSpecDeserializer.class)
public interface NessieTypeSpec extends Hashable {

  @Value.NonAttribute
  @Value.Default
  NessieType type();

  @Value.NonAttribute
  @Value.Default
  default String asString() {
    StringBuilder sb = new StringBuilder();
    return asString(sb).toString();
  }

  default StringBuilder asString(StringBuilder targetBuffer) {
    return targetBuffer.append(type().lowerCaseName());
  }

  // TODO The `NessieTypeSpec` type hierarchy uses polymorphism, which works fine for JSON.
  //  Protobuf: Polymorphism, especially with such a huge amount of types, does not work well ("look
  //    great") in a protobuf schema, because each subtype must be reflected as a `oneof` field.
  //  Avro: Polymorphism is barely doable in file formats that do not support named types but only
  //    structures.
  //  To also support Protobuf and Avro, types are serialized using a single type.
  // TODO Avro might still not work for nested types (think: a quite nested type like
  //  `object<map<map<object<list<object>,list<object>>>,object>,object>`).
  class NessieTypeSpecSerializer extends JsonSerializer<NessieTypeSpec> {
    @Override
    public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType type)
        throws JsonMappingException {
      visitor.expectObjectFormat(
          visitor.getProvider().getTypeFactory().constructType(NessieTypeSpecSerialized.class));
    }

    @Override
    public void serialize(NessieTypeSpec value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeObject(NessieTypeSpecSerialized.wrap(value));
    }
  }

  class NessieTypeSpecDeserializer extends JsonDeserializer<NessieTypeSpec> {
    @Override
    public NessieTypeSpec deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      return p.readValueAs(NessieTypeSpecSerialized.class).unwrap();
    }
  }
}
