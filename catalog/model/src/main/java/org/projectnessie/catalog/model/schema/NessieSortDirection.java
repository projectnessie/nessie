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
package org.projectnessie.catalog.model.schema;

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
@JsonSerialize(using = NessieSortDirection.NessieSortDirectionSerializer.class)
@JsonDeserialize(using = NessieSortDirection.NessieSortDirectionDeserializer.class)
public interface NessieSortDirection {
  String name();

  String jsonValue();

  String ASC_VALUE = "asc";
  String DESC_VALUE = "desc";
  NessieSortDirection ASC = ImmutableNessieSortDirection.of("ASC", ASC_VALUE);
  NessieSortDirection DESC = ImmutableNessieSortDirection.of("DESC", DESC_VALUE);

  class NessieSortDirectionSerializer extends JsonSerializer<NessieSortDirection> {
    @Override
    public void serialize(
        NessieSortDirection value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(value.jsonValue());
    }
  }

  class NessieSortDirectionDeserializer extends JsonDeserializer<NessieSortDirection> {
    @Override
    public NessieSortDirection deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String text = p.getText();
      switch (text) {
        case ASC_VALUE:
          return ASC;
        case DESC_VALUE:
          return DESC;
        default:
          return ImmutableNessieSortDirection.of(text, text);
      }
    }
  }
}
