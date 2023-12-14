/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonSerialize(as = ImmutableRepositoryDescription.class)
@JsonDeserialize(as = ImmutableRepositoryDescription.class)
public interface RepositoryDescription {

  static Builder builder() {
    return ImmutableRepositoryDescription.builder();
  }

  interface Builder {
    Builder repositoryCreatedTime(Instant repositoryCreatedTime);

    Builder oldestPossibleCommitTime(Instant oldestPossibleCommitTime);

    Builder putProperties(String key, String value);

    Builder putAllProperties(Map<String, ? extends String> entries);

    Builder defaultBranchName(String defaultBranchName);

    Builder repositoryImportedTime(Instant repositoryImportedTime);

    RepositoryDescription build();
  }

  /** The timestamp when the repository has been created. */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant repositoryCreatedTime();

  /**
   * The timestamp of the oldest commit.
   *
   * <p>If the repository has been imported, this value will be different from {@link
   * #repositoryCreatedTime()}.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant oldestPossibleCommitTime();

  Map<String, String> properties();

  String defaultBranchName();

  /**
   * The timestamp when the repository has been fully imported. This information is only present if
   * the repository was imported using a recent version of Nessie CLI.
   */
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  @Nullable
  Instant repositoryImportedTime();

  /**
   * Used to serialize an instant to ISO-8601 format. Required because not all platforms we work
   * with support jackson's jdk8 modules.
   */
  class InstantSerializer extends StdSerializer<Instant> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

    @SuppressWarnings("unused")
    public InstantSerializer() {
      this(Instant.class);
    }

    protected InstantSerializer(Class<Instant> t) {
      super(t);
    }

    @Override
    public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeString(FORMATTER.format(value));
    }
  }

  /**
   * Used to deserialize an instant to ISO-8601 format. Required because not all platforms we work
   * with support jackson's jdk8 modules.
   */
  class InstantDeserializer extends StdDeserializer<Instant> {
    @SuppressWarnings("unused")
    public InstantDeserializer() {
      this(null);
    }

    protected InstantDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return Instant.parse(p.getText());
    }
  }
}
