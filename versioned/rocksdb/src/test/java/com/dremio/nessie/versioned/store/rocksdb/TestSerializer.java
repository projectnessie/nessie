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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.dremio.nessie.versioned.store.Entity;
import com.esotericsoftware.kryo.io.Output;

class TestSerializer {
  static final TestSerializer SERIALIZER = new TestSerializer();

  byte[] toBytes(Map<String, Entity> value) {
    return toBytes(o -> {
      o.writeInt(value.size());
      value.forEach((k, v) -> {
        o.writeString(k);
        o.writeBytes(toBytes(v));
      });
    });
  }

  private byte[] toBytes(List<Entity> value) {
    return toBytes(o -> {
      o.writeInt(value.size());
      value.forEach(v -> o.writeBytes(toBytes(v)));
    });
  }

  private byte[] toBytes(Entity entity) {
    return toBytes(o -> {
      o.writeShort(entity.getType().ordinal());
      switch (entity.getType()) {
        case MAP:
          o.writeBytes(toBytes(entity.getMap()));
          break;
        case LIST:
          o.writeBytes(toBytes(entity.getList()));
          break;
        case NUMBER:
          o.writeLong(entity.getNumber());
          break;
        case STRING:
          o.writeString(entity.getString());
          break;
        case BOOLEAN:
          o.writeBoolean(entity.getBoolean());
          break;
        case BINARY:
          o.writeInt(entity.getBinary().size());
          o.writeBytes(entity.getBinary().toByteArray());
          break;
        default:
          throw new UnsupportedOperationException(String.format("Unsupported field type: %s", entity.getType().name()));
      }
    });
  }

  private byte[] toBytes(Consumer<Output> consumer) {
    final Output output = new Output(RocksDBStore.OUTPUT_SIZE);
    consumer.accept(output);
    return output.toBytes();
  }
}
