/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.meta;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.IOException;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(using = IcebergNamespace.IcebergNamespaceSerializer.class)
@JsonDeserialize(using = IcebergNamespace.IcebergNamespaceDeserializer.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergNamespace {
  @Value.Parameter(order = 1)
  List<String> levels();

  IcebergNamespace EMPTY_ICEBERG_NAMESPACE = icebergNamespace(emptyList());

  static IcebergNamespace fromNessieNamespace(Namespace ns) {
    return icebergNamespace(ns.getElements());
  }

  static IcebergNamespace icebergNamespace(List<String> levels) {
    return ImmutableIcebergNamespace.of(levels);
  }

  static Builder builder() {
    return ImmutableIcebergNamespace.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergNamespace instance);

    @CanIgnoreReturnValue
    Builder addLevel(String element);

    @CanIgnoreReturnValue
    Builder addLevels(String... elements);

    @CanIgnoreReturnValue
    Builder levels(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addAllLevels(Iterable<String> elements);

    IcebergNamespace build();
  }

  default Namespace toNessieNamespace() {
    return Namespace.of(levels());
  }

  class IcebergNamespaceDeserializer extends JsonDeserializer<IcebergNamespace> {
    @Override
    public IcebergNamespace deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      checkArgument(p.currentToken() == JsonToken.START_ARRAY);
      Builder b = builder();
      while (true) {
        switch (p.nextToken()) {
          case VALUE_STRING:
            b.addLevel(p.getText());
            break;
          case END_ARRAY:
            return b.build();
          default:
            throw new IllegalArgumentException("Unexpected token " + p.currentToken());
        }
      }
    }
  }

  class IcebergNamespaceSerializer extends JsonSerializer<IcebergNamespace> {
    @Override
    public void serialize(
        IcebergNamespace namespace, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartArray();
      for (String level : namespace.levels()) {
        gen.writeString(level);
      }
      gen.writeEndArray();
    }
  }
}
