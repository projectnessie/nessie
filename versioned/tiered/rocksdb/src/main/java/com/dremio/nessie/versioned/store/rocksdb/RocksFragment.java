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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.InvalidProtocolBufferException;

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
  public boolean evaluate(Condition condition) {
    for (Function function: condition.getFunctionList()) {
      // Retrieve entity at function.path
      List<String> path = Evaluator.splitPath(function.getPath());
      String segment = path.get(0);
      switch (segment) {
        case ID:
          if (!(path.size() == 1
            && function.getOperator().equals(Function.EQUALS)
            && getId().toEntity().equals(function.getValue()))) {
            return false;
          }
          break;
        case KEY_LIST:
          if (path.size() != 1) {
            return false;
          }
          if (function.getOperator().equals(Function.EQUALS)) {
            if (!keysAsEntityList(keys).equals(function.getValue())) {
              return false;
            }
          } else if (function.getOperator().equals(Function.SIZE)) {
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
    List<Key> keyListIn = keys.collect(Collectors.toList());
    List<Entity> resultList = new ArrayList<>();
    for (Key key : keyListIn) {
      List<Entity> entityElementList = new ArrayList<>();
      for (String element : key.getElements()) {
        entityElementList.add(Entity.ofString(element));
      }
      resultList.add(Entity.ofList(entityElementList));
    }
    return Entity.ofList(resultList);
  }
}
