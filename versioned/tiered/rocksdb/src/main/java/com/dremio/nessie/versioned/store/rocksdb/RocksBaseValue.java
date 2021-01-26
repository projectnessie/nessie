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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;

abstract class RocksBaseValue<C extends BaseValue<C>> implements BaseValue<C> {

  private Id id;
  private long dt;

  RocksBaseValue(Id id, long dt) {
    this.id = id;
    this.dt = dt;
  }

  @SuppressWarnings("unchecked")
  @Override
  public C id(Id id) {
    this.id = id;
    return (C) this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public C dt(long dt) {
    this.dt = dt;
    return (C) this;
  }

  Id getId() {
    return id;
  }

  /**
   * Splits a string containing an identifier and an array position enclosed in parentheses into a list.
   * @param str the string to split. Format: "value0(value1)"
   * @return a list containing the values
   */
  List<String> splitArrayString(String str) {
    // Remove enclosing brackets
    final String delimeters = "\\(|\\)";
    return Arrays.asList(str.split(delimeters));
  }

  /**
   * Converts a Stream of Ids into a List Entity.
   * @param idStream the stream of Id to convert.
   * @return the List Entity.
   */
  Entity toEntity(Stream<Id> idStream) {
    List<Id> idList = idStream.collect(Collectors.toList());
    List<Entity> idsAsEntity = new ArrayList<>();
    for (Id idElement : idList) {
      idsAsEntity.add(idElement.toEntity());
    }
    return Entity.ofList(idsAsEntity);
  }

  /**
   * Retrieves an Id at 'position' in a Stream of Ids as an Entity.
   * @param idStream the stream of Id to convert.
   * @param position the element in the Stream to retrieve.
   * @return the List Entity.
   */
  Entity toEntity(Stream<Id> idStream, int position) {
    return idStream.collect(Collectors.toList()).get(position).toEntity();
  }
}
