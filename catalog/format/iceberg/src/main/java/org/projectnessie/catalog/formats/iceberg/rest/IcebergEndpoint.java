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
package org.projectnessie.catalog.formats.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(using = IcebergEndpoint.IcebergEndpointSerializer.class)
@tools.jackson.databind.annotation.JsonSerialize(
    using = IcebergEndpoint.IcebergEndpointSerializer3.class)
@JsonDeserialize(using = IcebergEndpoint.IcebergEndpointDeserializer.class)
@tools.jackson.databind.annotation.JsonDeserialize(
    using = IcebergEndpoint.IcebergEndpointDeserializer3.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@tools.jackson.databind.annotation.JsonNaming(
    tools.jackson.databind.PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergEndpoint {
  String httpMethod();

  String path();

  static IcebergEndpoint icebergEndpoint(String httpMethod, String path) {
    return ImmutableIcebergEndpoint.of(httpMethod, path);
  }

  class IcebergEndpointSerializer extends JsonSerializer<IcebergEndpoint> {
    @Override
    public void serialize(IcebergEndpoint value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(value.httpMethod() + " " + value.path());
    }
  }

  class IcebergEndpointDeserializer extends JsonDeserializer<IcebergEndpoint> {
    @Override
    public IcebergEndpoint deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String val = p.getValueAsString();
      int i = val.indexOf(' ');
      String method = val.substring(0, i);
      String path = val.substring(i + 1);
      return icebergEndpoint(method, path);
    }
  }

  class IcebergEndpointSerializer3 extends tools.jackson.databind.ValueSerializer<IcebergEndpoint> {
    @Override
    public void serialize(
        IcebergEndpoint value,
        tools.jackson.core.JsonGenerator gen,
        tools.jackson.databind.SerializationContext serializers)
        throws tools.jackson.core.JacksonException {
      gen.writeString(value.httpMethod() + " " + value.path());
    }
  }

  class IcebergEndpointDeserializer3
      extends tools.jackson.databind.ValueDeserializer<IcebergEndpoint> {
    @Override
    public IcebergEndpoint deserialize(
        tools.jackson.core.JsonParser p, tools.jackson.databind.DeserializationContext ctxt)
        throws tools.jackson.core.JacksonException {
      String val = p.getValueAsString();
      int i = val.indexOf(' ');
      String method = val.substring(0, i);
      String path = val.substring(i + 1);
      return icebergEndpoint(method, path);
    }
  }
}
