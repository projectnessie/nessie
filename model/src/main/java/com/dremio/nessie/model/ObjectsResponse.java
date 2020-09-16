package com.dremio.nessie.model;

import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableObjectsResponse.class)
@JsonDeserialize(as = ImmutableObjectsResponse.class)
public interface ObjectsResponse extends PaginatedResponse {

  List<Entry> getEntries();

  @Value.Immutable(prehash = true)
  @JsonSerialize(as = ImmutableEntry.class)
  @JsonDeserialize(as = ImmutableEntry.class)
  interface Entry {

    Contents.Type getType();

    NessieObjectKey getName();
  }
}
