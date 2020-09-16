package com.dremio.nessie.model;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableSqlView.class)
@JsonDeserialize(as = ImmutableSqlView.class)
@JsonTypeName("VIEW")
public interface SqlView extends Contents {

  public static enum Dialect {
    HIVE,
    SPARK,
    DREMIO,
    PRESTO
  }

  String getSqlText();

  Dialect getDialect();

  // Schema getSchema();
}
