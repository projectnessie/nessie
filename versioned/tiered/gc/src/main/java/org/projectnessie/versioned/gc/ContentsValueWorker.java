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
package org.projectnessie.versioned.gc;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.apache.spark.util.SerializableConfiguration;

import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.providers.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.AssetKey;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.ValueWorker;
import org.projectnessie.versioned.gc.assets.FileSystemAssetKey;
import org.projectnessie.versioned.gc.assets.IcebergAssetKeyReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.google.protobuf.ByteString;

/**
 * ValueWorker which wraps the default worker for reading a Contents.
 *
 * <p>This augments the default worker by making it Serializable and by allowing it to iterate AssetKeys.
 * Note we use Externalizable here to prevent Spark from trying to serialize the inner value worker
 */
class ContentsValueWorker implements ValueWorker<Contents>, Externalizable {
  private final ValueWorker<Contents> innerWorker = new TableCommitMetaStoreWorker().getValueWorker();
  private SerializableConfiguration configuration; // keep it serializable so we can move it around spark

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
      return new IcebergAssetKeyReader<>(contents.unwrap(IcebergTable.class).get(), configuration).get();
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

  /**
   * Serialize an Asset Key to json and deserialize it. Add extra configuration to AssetKey where appropriate.
   *
   * <p>We use a specialized object mapper to persist with classname w/o annotations. Ideally this data should
   * not be persisted and only used to move around Spark. If it is the ObjectMapper needs to be configured the same way.
   */
  private static class AssetKeySerializer implements Serializer<AssetKey>, Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper().activateDefaultTypingAsProperty(
        BasicPolymorphicTypeValidator.builder().allowIfBaseType(AssetKey.class).build(),
        ObjectMapper.DefaultTyping.NON_FINAL,
        "clazz");

    private final SerializableConfiguration configuration;

    public AssetKeySerializer(SerializableConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public ByteString toBytes(AssetKey value) {
      return ByteString.copyFrom(toJson(value).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public AssetKey fromBytes(ByteString bytes) {
      AssetKey key = fromJson(bytes.toByteArray(), AssetKey.class);

      if (key instanceof FileSystemAssetKey) {
        ((FileSystemAssetKey) key).setHadoopConf(configuration);
      }
      return key;
    }

    private static String toJson(AssetKey value) {
      try {
        return MAPPER.writeValueAsString(value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
          String.format("Cannot serialise asset key of type %s with id %s", value.getClass(), value.toReportableName()), e);
      }
    }

    private static <T extends AssetKey> AssetKey fromJson(byte[] value, Class<T> type) {
      try {
        return MAPPER.readValue(value, type);
      } catch (IOException e) {
        throw new RuntimeException(
          String.format("Cannot deserialise asset key with json %s", new String(value)), e);
      }
    }
  }
}
