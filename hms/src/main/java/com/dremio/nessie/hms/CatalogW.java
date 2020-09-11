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
package com.dremio.nessie.hms;

import org.apache.hadoop.hive.metastore.api.Catalog;

class CatalogW implements Item {

  private final Catalog catalog;

  public CatalogW(Catalog catalog) {
    super();
    this.catalog = catalog;
  }

  @Override
  public Type getType() {
    return Type.CATALOG;
  }

  @Override
  public Catalog getCatalog() {
    return catalog;
  }



}
