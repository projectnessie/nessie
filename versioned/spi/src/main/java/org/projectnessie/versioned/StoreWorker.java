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
package org.projectnessie.versioned;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;

/**
 * A set of helpers that users of a VersionStore must implement.
 *
 * @param <VALUE> The value type saved in the VersionStore.
 * @param <COMMIT_METADATA> The commit metadata type saved in the VersionStore.
 */
public interface StoreWorker<VALUE, COMMIT_METADATA> {

  ValueWorker<WithEntityType<VALUE>> getValueWorker();

  Serializer<COMMIT_METADATA> getMetadataSerializer();

  /**
   * Create StoreWorker for provided helpers. Adding single byte data type as first byte in serialized value.
   */
  static <VALUE, COMMIT_METADATA> StoreWorker<VALUE, COMMIT_METADATA>
      of(ValueWorker<VALUE> valueWorker, Serializer<COMMIT_METADATA> commitSerializer) {
    return new StoreWorker<VALUE, COMMIT_METADATA>() {

      @Override
      public ValueWorker<WithEntityType<VALUE>> getValueWorker() {
        return new ValueWorker<WithEntityType<VALUE>>() {
          @Override
          public Stream<? extends AssetKey> getAssetKeys(WithEntityType<VALUE> valueWithEntityType) {
            return valueWorker.getAssetKeys(valueWithEntityType.getValue());
          }

          @Override
          public Serializer<AssetKey> getAssetKeySerializer() {
            return valueWorker.getAssetKeySerializer();
          }

          @Override
          public ByteString toBytes(WithEntityType<VALUE> value) {
            return ByteString.copyFrom(ByteBuffer.allocate(4).putInt(value.getEntityType()).array())
                .concat(valueWorker.toBytes(value.getValue()));
          }

          @Override
          public WithEntityType<VALUE> fromBytes(ByteString bytes) {
            return WithEntityType.of(bytes.substring(0, 4).asReadOnlyByteBuffer().getInt(),
                valueWorker.fromBytes(bytes.substring(4)));
          }
        };
      }

      @Override
      public Serializer<COMMIT_METADATA> getMetadataSerializer() {
        return commitSerializer;
      }

    };
  }

}
