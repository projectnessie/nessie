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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;
import org.projectnessie.model.ser.CommitMetaDeserializer;
import org.projectnessie.model.ser.Views;

@Value.Immutable
@Schema(
    type = SchemaType.OBJECT,
    title = "CommitMeta",
    // Smallrye does neither support JsonFormat nor javax.validation.constraints.Pattern :(
    properties = {@SchemaProperty(name = "hash", pattern = Validation.HASH_REGEX)})
@JsonSerialize(as = ImmutableCommitMeta.class)
@JsonDeserialize(using = CommitMetaDeserializer.class)
public abstract class CommitMeta {

  /**
   * Hash of this commit.
   *
   * <p>This is not known at creation time and is only valid when reading the log.
   */
  @Nullable
  @jakarta.annotation.Nullable
  public abstract String getHash();

  /**
   * The entity performing this commit/transaction.
   *
   * <p>This is the logged in user/account who performs this action. Populated on the server. Nessie
   * will return an error if this is populated by the client side.
   *
   * <p>The committer should follow the git spec for names eg Committer Name
   * &lt;committer.name@example.com&gt; but this is not enforced. See <a
   * href="https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---authorltauthorgt">git-commit
   * docs</a>.
   */
  @Nullable
  @jakarta.annotation.Nullable
  public abstract String getCommitter();

  /** The author of a commit. This is the original committer. */
  @Nullable
  @jakarta.annotation.Nullable
  @Value.Derived
  @JsonView(Views.V1.class)
  public String getAuthor() {
    return getAllAuthors().isEmpty() ? null : getAllAuthors().get(0);
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  @JsonView(Views.V2.class)
  @JsonProperty("authors")
  public abstract List<String> getAllAuthors();

  /**
   * Authorizer of this action.
   *
   * <p>For example if the user who did the transaction is a service account this could be populated
   * by the person who started the job.
   */
  @Nullable
  @jakarta.annotation.Nullable
  @Value.Derived
  @JsonView(Views.V1.class)
  public String getSignedOffBy() {
    return getAllSignedOffBy().isEmpty() ? null : getAllSignedOffBy().get(0);
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  @JsonView(Views.V2.class)
  public abstract List<String> getAllSignedOffBy();

  /**
   * Message describing this commit. Should conform to Git style.
   *
   * <p>Like github if this message is in markdown it may be displayed cleanly in the UI.
   */
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  public abstract String getMessage();

  /** Commit time in UTC. Set by the server. */
  @Nullable
  @jakarta.annotation.Nullable
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  public abstract Instant getCommitTime();

  /** Original commit time in UTC. Set by the server. */
  @Nullable
  @jakarta.annotation.Nullable
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  public abstract Instant getAuthorTime();

  /**
   * Set of properties to help further identify this commit.
   *
   * <p>examples are spark id, the client type (eg iceberg, delta etc), application or job names,
   * hostnames etc.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  @Value.NonAttribute
  @JsonView(Views.V1.class)
  public Map<String, String> getProperties() {
    HashMap<String, String> firstElements = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : getAllProperties().entrySet()) {
      List<String> list = entry.getValue();
      if (!list.isEmpty()) {
        firstElements.put(entry.getKey(), list.get(0));
      }
    }
    return firstElements;
  }

  @NotNull
  @jakarta.validation.constraints.NotNull
  @JsonView(Views.V2.class)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public abstract Map<String, List<String>> getAllProperties();

  @NotNull
  @jakarta.validation.constraints.NotNull
  @JsonView(Views.V2.class)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public abstract List<String> getParentCommitHashes();

  public ImmutableCommitMeta.Builder toBuilder() {
    return ImmutableCommitMeta.builder().from(this);
  }

  public static ImmutableCommitMeta.Builder builder() {
    return ImmutableCommitMeta.builder();
  }

  public static CommitMeta fromMessage(String message) {
    return ImmutableCommitMeta.builder().message(message).build();
  }

  public interface Builder {

    Builder message(String message);

    default ImmutableCommitMeta.Builder author(String author) {
      if (author != null) {
        return addAllAuthors(author);
      }

      return (ImmutableCommitMeta.Builder) this;
    }

    ImmutableCommitMeta.Builder addAllAuthors(String author);

    default ImmutableCommitMeta.Builder signedOffBy(String author) {
      if (author != null) {
        return addAllSignedOffBy(author);
      }

      return (ImmutableCommitMeta.Builder) this;
    }

    ImmutableCommitMeta.Builder addAllSignedOffBy(String author);

    default ImmutableCommitMeta.Builder properties(Map<String, ? extends String> entries) {
      allProperties(Collections.emptyMap()); // clear all properties
      return putAllProperties(entries);
    }

    default ImmutableCommitMeta.Builder putAllProperties(Map<String, ? extends String> entries) {
      for (Map.Entry<String, ? extends String> entry : entries.entrySet()) {
        putProperties(entry.getKey(), entry.getValue());
      }
      return (ImmutableCommitMeta.Builder) this;
    }

    default ImmutableCommitMeta.Builder putProperties(Map.Entry<String, ? extends String> entry) {
      return putProperties(entry.getKey(), entry.getValue());
    }

    default ImmutableCommitMeta.Builder putProperties(String key, String value) {
      return putAllProperties(key, Collections.singletonList(value));
    }

    ImmutableCommitMeta.Builder putAllProperties(String key, List<String> value);

    ImmutableCommitMeta.Builder allProperties(Map<String, ? extends List<String>> entries);

    CommitMeta build();
  }

  /**
   * Used to serialize an instant to ISO-8601 format. Required because not all platforms we work
   * with support jackson's jdk8 modules.
   */
  public static class InstantSerializer extends StdSerializer<Instant> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

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
  public static class InstantDeserializer extends StdDeserializer<Instant> {
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
