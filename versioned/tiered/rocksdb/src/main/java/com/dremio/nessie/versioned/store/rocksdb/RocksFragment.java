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

import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link com.dremio.nessie.tiered.builder.Fragment} providing
 * SerDe and Condition evaluation.
 */
class RocksFragment extends RocksBaseValue<Fragment> implements Fragment, Evaluator {
  static final String KEY_LIST = "keys";

  private Stream<Key> keys;

  RocksFragment() {
    super();
  }

  @Override
  public Fragment keys(Stream<Key> keys) {
    this.keys = keys;
    return this;
  }

  @Override
  byte[] build() {
    checkPresent(keys, KEY_LIST);
    return ValueProtos.Fragment.newBuilder()
      .setBase(buildBase())
      .addAllKeys(keys.map(RocksBaseValue::buildKey).collect(Collectors.toList()))
      .build()
      .toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static void toConsumer(byte[] value, Fragment consumer) {
    try {
      final ValueProtos.Fragment fragment = ValueProtos.Fragment.parseFrom(value);
      setBase(consumer, fragment.getBase());
      consumer.keys(fragment.getKeysList().stream().map(RocksBaseValue::createKey));
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt Fragment value encountered when deserializing.", e);
    }
  }

  @Override
  public boolean evaluateSegment(ExpressionPath.NameSegment nameSegment, Function function) {
    final String segment = nameSegment.getName();
    switch (segment) {
      case ID:
        if (!idEvaluates(nameSegment, function)) {
          return false;
        }
        break;
      case KEY_LIST:
        if (nameSegment.getChild().isPresent()) {
          return false;
        }
        if (function.isEquals()) {
          if (!keysAsEntityList(keys).equals(function.getValue())) {
            return false;
          }
        } else if (function.isSize()) {
          if (keys.count() != function.getValue().getNumber()) {
            return false;
          }
        } else {
          return false;
        }
        break;
      default:
        // Invalid Condition Function.
        return false;
    }
    return true;
  }

  /**
   * Produces an Entity List of Entity Lists for keys.
   * Each key is represented as an entity list of string entities.
   * @param keys stream of keys to convert
   * @return an entity list of keys
   */
  private Entity keysAsEntityList(Stream<Key> keys) {
    return Entity.ofList(keys.map(k -> Entity.ofList(k.getElements().stream().map(Entity::ofString))));
  }
}
