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
@JsonSerialize(using = NessieNullOrder.NessieNullOrderSerializer.class)
@JsonDeserialize(using = NessieNullOrder.NessieNullOrderDeserializer.class)
public interface NessieNullOrder {
  String name();

  String jsonValue();

  String NULLS_FIRST_VALUE = "nulls-first";
  String NULLS_LAST_VALUE = "nulls-last";
  NessieNullOrder NULLS_FIRST = ImmutableNessieNullOrder.of("NULLS_FIRST", NULLS_FIRST_VALUE);
  NessieNullOrder NULLS_LAST = ImmutableNessieNullOrder.of("NULLS_LAST", NULLS_LAST_VALUE);

  class NessieNullOrderSerializer extends JsonSerializer<NessieNullOrder> {
    @Override
    public void serialize(NessieNullOrder value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(value.jsonValue());
    }
  }

  class NessieNullOrderDeserializer extends JsonDeserializer<NessieNullOrder> {
    @Override
    public NessieNullOrder deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String text = p.getText();
      switch (text) {
        case NULLS_FIRST_VALUE:
          return NULLS_FIRST;
        case NULLS_LAST_VALUE:
          return NULLS_LAST;
        default:
          return ImmutableNessieNullOrder.of(text, text);
      }
    }
  }
}
