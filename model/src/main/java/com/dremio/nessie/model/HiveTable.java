package com.dremio.nessie.model;

import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableHiveTable.class)
@JsonDeserialize(as = ImmutableHiveTable.class)
@JsonTypeName("HIVE_TABLE")
public interface HiveTable extends Contents {

  byte[] getTableDefinition();

  List<byte[]> getPartitions();

}
