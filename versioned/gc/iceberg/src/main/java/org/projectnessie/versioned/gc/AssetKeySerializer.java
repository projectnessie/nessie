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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.Serializable;
import org.apache.spark.util.SerializableConfiguration;
import org.projectnessie.versioned.Serializer;

/**
 * Serialize an Asset Key to json and deserialize it. Add extra configuration to AssetKey where
 * appropriate.
 *
 * <p>We use a specialized object mapper to persist with classname w/o annotations. Ideally this
 * data should not be persisted and only used to move around Spark. If it is the ObjectMapper needs
 * to be configured the same way.
 */
public class AssetKeySerializer implements Serializer<AssetKey>, Serializable {

  private final SerializableConfiguration configuration;

  public AssetKeySerializer(SerializableConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ByteString toBytes(AssetKey value) {
    Preconditions.checkArgument(
        value instanceof IcebergAssetKey, "Cannot deserialize non-iceberg Asset Keys");
    IcebergAssetKey key = (IcebergAssetKey) value;
    ObjectTypes.PIcebergAssetKey protoKey =
        ObjectTypes.PIcebergAssetKey.newBuilder()
            .setPath(key.getPath())
            .setSnapshotId(Long.parseLong(key.getSnapshotId()))
            .setTableName(key.getTableName())
            .setType(key.getType().toString())
            .build();
    return protoKey.toByteString();
  }

  @Override
  public AssetKey fromBytes(ByteString bytes) {
    try {
      ObjectTypes.PIcebergAssetKey protoKey = ObjectTypes.PIcebergAssetKey.parseFrom(bytes);
      IcebergAssetKey key =
          new IcebergAssetKey(
              protoKey.getPath(),
              configuration,
              IcebergAssetKey.AssetKeyType.valueOf(protoKey.getType()),
              protoKey.getSnapshotId(),
              protoKey.getTableName());
      return key;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "could not parse protobuf string for IcebergAssetKey, is this an iceberg key?", e);
    }
  }
}
