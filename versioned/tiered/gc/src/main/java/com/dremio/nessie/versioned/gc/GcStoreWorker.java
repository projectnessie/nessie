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
package com.dremio.nessie.versioned.gc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.server.providers.TableCommitMetaStoreWorker;
import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.ValueWorker;
import com.dremio.nessie.versioned.gc.assets.FileSystemAssetKey;
import com.dremio.nessie.versioned.gc.iceberg.IcebergAssetKeyReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import scala.Array;

public class GcStoreWorker implements StoreWorker<Contents, CommitMeta> {
  private final ValueWorker<Contents> valueWorker;
  private final Serializer<CommitMeta> metadataSerializer = new CommitMetaValueSerializer();

  public GcStoreWorker(Configuration hadoopConfig) {
    valueWorker = new ContentsValueWorker(new SerializableConfiguration(hadoopConfig));
  }

  @Override
  public ValueWorker<Contents> getValueWorker() {
    return valueWorker;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metadataSerializer;
  }

  private static class ContentsValueWorker implements ValueWorker<Contents>, Externalizable {
    private final ValueWorker<Contents> innerWorker = new TableCommitMetaStoreWorker().getValueWorker();
    private SerializableConfiguration configuration;

    public ContentsValueWorker() {

    }

    public ContentsValueWorker(SerializableConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public ByteString toBytes(Contents value) {
      return innerWorker.toBytes(value);
    }

    @Override
    public Contents fromBytes(ByteString bytes) {
      return innerWorker.fromBytes(bytes);
    }

    @Override
    public Stream<? extends AssetKey> getAssetKeys(Contents contents) {
      if (contents instanceof IcebergTable) {
        return new IcebergAssetKeyReader<>(contents.unwrap(IcebergTable.class).get(), configuration).getKeys();
      } else {
        return innerWorker.getAssetKeys(contents);
      }
    }


    @Override
    public Serializer<AssetKey> getAssetKeySerializer() {
      return new AssetKeySerializer(configuration);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
      objectOutput.writeObject(configuration);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
      configuration = (SerializableConfiguration) objectInput.readObject();
    }
  }

  private static class CommitMetaValueSerializer implements Serializer<CommitMeta>, Externalizable {
    private final Serializer<CommitMeta> innerWorker = new TableCommitMetaStoreWorker().getMetadataSerializer();

    public CommitMetaValueSerializer() {
    }

    @Override
    public ByteString toBytes(CommitMeta value) {
      return innerWorker.toBytes(value);
    }

    @Override
    public CommitMeta fromBytes(ByteString bytes) {
      return innerWorker.fromBytes(bytes);
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

    }
  }

  private static class AssetKeySerializer implements Serializer<AssetKey>, Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final SerializableConfiguration configuration;

    public AssetKeySerializer(SerializableConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public ByteString toBytes(AssetKey value) {
      String valueStr = toJson(value);
      byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
      int length = valueBytes.length + 1; // length of object plus 1 byte for type
      byte[] serializedBytes = new byte[length];
      Array.copy(valueBytes, 0, serializedBytes, 1, valueBytes.length);

      if (value instanceof FileSystemAssetKey) {
        serializedBytes[0] = 1;
      } else {
        serializedBytes[0] = 0;
      }
      return ByteString.copyFrom(serializedBytes);
    }

    private static String toJson(AssetKey value) {
      try {
        return MAPPER.writeValueAsString(value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    private static <T extends AssetKey> AssetKey fromJson(byte[] value, Class<T> type) {
      try {
        return MAPPER.readValue(value, type);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }


    @Override
    public AssetKey fromBytes(ByteString bytes) {
      byte type = bytes.byteAt(0);
      if (type == 1) {
        FileSystemAssetKey key = (FileSystemAssetKey) fromJson(bytes.substring(1, bytes.size()).toByteArray(), FileSystemAssetKey.class);
        key.setHadoopConf(configuration);
        return key;
      } else {
        return fromJson(bytes.substring(1, bytes.size()).toByteArray(), AssetKey.NoOpAssetKey.class);
      }
    }
  }
}
