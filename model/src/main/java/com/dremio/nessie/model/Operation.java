package com.dremio.nessie.model;

import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Schema(
    type = SchemaType.OBJECT,
    title = "Operation",
    oneOf = { Operation.Put.class, Operation.Unchanged.class, Operation.Delete.class},
    discriminatorMapping = {
        @DiscriminatorMapping(value = "PUT", schema = Operation.Put.class),
        @DiscriminatorMapping(value = "UNCHANGED", schema = Operation.Unchanged.class),
        @DiscriminatorMapping(value = "DELETE", schema = Operation.Delete.class)
    },
    discriminatorProperty = "type"
  )
@JsonSubTypes({
    @Type(Operation.Put.class),
    @Type(Operation.Delete.class),
    @Type(Operation.Unchanged.class)
  })
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface Operation {

  NessieObjectKey getKey();

  @Value.Immutable(prehash = true)
  @JsonSerialize(as = ImmutableCommitMeta.class)
  @JsonDeserialize(as = ImmutableCommitMeta.class)
  @JsonTypeName("PUT")
  interface Put extends Operation {
    Contents getObject();
  }

  @Value.Immutable(prehash = true)
  @JsonSerialize(as = ImmutableCommitMeta.class)
  @JsonDeserialize(as = ImmutableCommitMeta.class)
  @JsonTypeName("DELETE")
  interface Delete extends Operation {
  }

  @Value.Immutable(prehash = true)
  @JsonSerialize(as = ImmutableCommitMeta.class)
  @JsonDeserialize(as = ImmutableCommitMeta.class)
  @JsonTypeName("UNCHANGED")
  interface Unchanged extends Operation {
  }

}
