/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dremio.iceberg.server.db;

import java.util.Map;


import com.dremio.iceberg.model.Table;
import com.dremio.iceberg.model.Tables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class InMemory implements Backend {
  private static final Map<String, Table> tables = Maps.newHashMap();

  public InMemory() {
  }

  @Override
  public Table get(String name) {
    return tables.get(name);
  }

  @Override
  public Tables getAll() {
    return new Tables(ImmutableList.copyOf(tables.values()));
  }

  @Override
  public void create(String name, Table table) {
    if (tables.containsKey(name)) {
      throw new UnsupportedOperationException("Table " + name + " already exists");
    }
    tables.put(name, table);
  }

  @Override
  public void update(String name, Table table) {
    tables.put(name, table);
  }

  @Override
  public void remove(String name) {
    tables.remove(name);
  }

}
