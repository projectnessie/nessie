package com.dremio.nessie.model;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableHiveDatabase.class)
@JsonDeserialize(as = ImmutableHiveDatabase.class)
@JsonTypeName("HIVE_DATABASE")
public interface HiveDatabase extends Contents {

  byte[] getDatabaseDefinition();


}
