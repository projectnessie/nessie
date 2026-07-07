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
package org.projectnessie.catalog.formats.iceberg.metrics;

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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergTimerResult.class)
@tools.jackson.databind.annotation.JsonSerialize(as = ImmutableIcebergTimerResult.class)
@JsonDeserialize(as = ImmutableIcebergTimerResult.class)
@tools.jackson.databind.annotation.JsonDeserialize(as = ImmutableIcebergTimerResult.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@tools.jackson.databind.annotation.JsonNaming(
    tools.jackson.databind.PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergTimerResult {

  long count();

  @JsonSerialize(using = TimeUnitSerializer.class)
  @tools.jackson.databind.annotation.JsonSerialize(using = TimeUnitSerializer3.class)
  @JsonDeserialize(using = TimeUnitDeserializer.class)
  @tools.jackson.databind.annotation.JsonDeserialize(using = TimeUnitDeserializer3.class)
  TimeUnit timeUnit();

  long totalDuration();

  static Builder builder() {
    return ImmutableIcebergTimerResult.builder();
  }

  interface Builder {
    IcebergTimerResult build();
  }

  final class TimeUnitSerializer extends JsonSerializer<TimeUnit> {
    @Override
    public void serialize(TimeUnit value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeString(value.name().toLowerCase(Locale.ROOT));
    }
  }

  final class TimeUnitDeserializer extends JsonDeserializer<TimeUnit> {
    @Override
    public TimeUnit deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return TimeUnit.valueOf(p.getText().toUpperCase(Locale.ROOT));
    }
  }

  final class TimeUnitSerializer3 extends tools.jackson.databind.ValueSerializer<TimeUnit> {
    @Override
    public void serialize(
        TimeUnit value,
        tools.jackson.core.JsonGenerator gen,
        tools.jackson.databind.SerializationContext serializers)
        throws tools.jackson.core.JacksonException {
      gen.writeString(value.name().toLowerCase(Locale.ROOT));
    }
  }

  final class TimeUnitDeserializer3 extends tools.jackson.databind.ValueDeserializer<TimeUnit> {
    @Override
    public TimeUnit deserialize(
        tools.jackson.core.JsonParser p, tools.jackson.databind.DeserializationContext ctxt)
        throws tools.jackson.core.JacksonException {
      return TimeUnit.valueOf(p.getString().toUpperCase(Locale.ROOT));
    }
  }
}
