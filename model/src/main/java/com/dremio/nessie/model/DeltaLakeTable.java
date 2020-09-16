package com.dremio.nessie.model;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableDeltaLakeTable.class)
@JsonDeserialize(as = ImmutableDeltaLakeTable.class)
@JsonTypeName("DELTALAKE_TABLE")
public interface DeltaLakeTable extends Contents {

  String getMetadataLocation();
}
