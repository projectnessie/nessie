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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(using = IcebergDirection.IcebergDirectionSerializer.class)
@JsonDeserialize(using = IcebergDirection.IcebergDirectionDeserializer.class)
public interface IcebergDirection {
  String name();

  String jsonValue();

  String ASC_VALUE = "asc";
  String DESC_VALUE = "desc";
  IcebergDirection ASC = ImmutableIcebergDirection.of("ASC", ASC_VALUE);
  IcebergDirection DESC = ImmutableIcebergDirection.of("DESC", DESC_VALUE);

  class IcebergDirectionSerializer extends JsonSerializer<IcebergDirection> {
    @Override
    public void serialize(IcebergDirection value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(value.jsonValue());
    }
  }

  class IcebergDirectionDeserializer extends JsonDeserializer<IcebergDirection> {
    @Override
    public IcebergDirection deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String text = p.getText();
      switch (text) {
        case ASC_VALUE:
          return ASC;
        case DESC_VALUE:
          return DESC;
        default:
          return ImmutableIcebergDirection.of(text, text);
      }
    }
  }
}
