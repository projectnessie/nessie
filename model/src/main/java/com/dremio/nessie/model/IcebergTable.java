package com.dremio.nessie.model;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableIcebergTable.class)
@JsonDeserialize(as = ImmutableIcebergTable.class)
@JsonTypeName("ICEBERG_TABLE")
public interface IcebergTable extends Contents {

  String getMetadataLocation();

}
