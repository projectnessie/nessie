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
package org.projectnessie.versioned.tiered.gc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.protobuf.ByteString;

/**
 * A utility class wrapping binary bloom filter functionality for spark.
 *
 * <p>Has two different serialization formats:
 * <ol>
 * <li>format used when exposing a result to a row (direct use of Guava's {@code BloomFilter.readfrom} operations)
 * <li>internal format used when doing aggregation (based on {@link Externalizable})
 * </ol>
 *
 */
public class BinaryBloomFilter implements Externalizable {

  private static final Funnel<ByteBuffer> BYTE_FUNNEL = new BinaryFunnel();
  private static final int SIZE = 10_000_000;
  private static final Encoder<BinaryBloomFilter> ENCODER = Encoders.kryo(BinaryBloomFilter.class);

  private BloomFilter<ByteBuffer> filter;

  public BinaryBloomFilter() {
    this.filter = BloomFilter.create(BYTE_FUNNEL, SIZE);
  }

  public boolean mightContain(byte[] bytes) {
    return filter.mightContain(ByteBuffer.wrap(bytes));
  }

  public boolean mightContain(ByteString bytes) {
    return filter.mightContain(bytes.asReadOnlyByteBuffer());
  }

  public boolean mightContain(ByteBuffer bytes) {
    return filter.mightContain(bytes);
  }

  private static class BloomFilterAggregator extends
      org.apache.spark.sql.expressions.Aggregator<byte[], BinaryBloomFilter, byte[]> {
    private static final long serialVersionUID = -537765519777214377L;

    @Override
    public Encoder<BinaryBloomFilter> bufferEncoder() {
      return BinaryBloomFilter.ENCODER;
    }

    @Override
    public byte[] finish(BinaryBloomFilter reduction) {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        reduction.filter.writeTo(baos);
        baos.flush();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException("Unexpected exception while serializing to byte array.", e);
      }
    }

    @Override
    public BinaryBloomFilter merge(BinaryBloomFilter b1, BinaryBloomFilter b2) {
      b1.filter.putAll(b2.filter);
      return b1;
    }

    @Override
    public Encoder<byte[]> outputEncoder() {
      return Encoders.BINARY();
    }

    @Override
    public BinaryBloomFilter reduce(BinaryBloomFilter b, byte[] a) {
      b.filter.put(ByteBuffer.wrap(a));
      return b;
    }

    @Override
    public BinaryBloomFilter zero() {
      return new BinaryBloomFilter();
    }

  }

  private static BinaryBloomFilter fromBinary(byte[] bytes) {
    ByteArrayInputStream input = new ByteArrayInputStream(bytes);
    BinaryBloomFilter filter = new BinaryBloomFilter();
    try {
      filter.filter = BloomFilter.readFrom(input, BYTE_FUNNEL);
      return filter;
    } catch (IOException e) {
      throw new RuntimeException("Failure reading bloom filter.", e);
    }
  }

  /**
   * Run aggregation on dataset to add all rows to the bloom filter.
   * @param data a Spark Dataset to add to binary bloom filter
   * @param column the column to aggregate on
   */
  public static BinaryBloomFilter aggregate(Dataset<Row> data, String column) {
    Row[] aggregated = (Row[]) data.agg(functions.udaf(new BloomFilterAggregator(), Encoders.BINARY()).apply(data.col(column))).collect();
    byte[] bytes = aggregated[0].getAs(0);
    return fromBinary(bytes);
  }

  public Dataset<Row> filter(Dataset<Row> data, String column) {
    return data.filter(filter(this, column));
  }

  private static FilterFunction<Row> filter(BinaryBloomFilter bloomFilter, String column) {
    return f -> {
      byte[] bytes = f.getAs(column);
      return bloomFilter.mightContain(bytes);
    };
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    filter.writeTo(baos);
    baos.flush();
    byte[] bytes = baos.toByteArray();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int len = in.readInt();
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    this.filter = BloomFilter.readFrom(new ByteArrayInputStream(bytes), BYTE_FUNNEL);
  }

  private static class BinaryFunnel implements Funnel<ByteBuffer> {

    private static final long serialVersionUID = 1308914370144641865L;

    @Override
    public void funnel(ByteBuffer from, PrimitiveSink into) {
      into.putBytes(from);
    }

    @Override
    public int hashCode() {
      return 1;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof BinaryFunnel)) {
        return false;
      }

      return true;
    }

  }
}
