package com.dremio.versioned.gc;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.dremio.nessie.tiered.builder.Value;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;

public class ValueFrame {
  private byte[] bytes;
  private long dt;
  private IdFrame id;

  public byte[] getBytes() {
    return bytes;
  }

  public long getDt() {
    return dt;
  }

  public void setDt(long dt) {
    this.dt = dt;
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes;
  }

  public IdFrame getId() {
    return id;
  }

  public void setId(IdFrame id) {
    this.id = id;
  }

  public ValueFrame() {
  }

  public static Function<Store.Acceptor<Value>, ValueFrame> BUILDER = (a) -> {
    ValueFrame f = new ValueFrame();
    a.applyValue(new Value() {

      @Override
      public Value value(ByteString value) {
        f.bytes = value.toByteArray();
        return this;
      }

      @Override
      public Value id(Id id) {
        f.id = IdFrame.of(id);
        return this;
      }

      @Override
      public Value dt(long dt) {
        f.dt = dt;
        return this;
      }
    });
    return f;
  };

  public static Dataset<ValueFrame> asDataset(Supplier<Store> store, SparkSession spark) {
    return ValueRetriever.dataset(store, ValueType.VALUE, ValueFrame.class, Optional.empty(), spark, BUILDER);
  }

}
