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

package com.dremio.nessie.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;

public final class NessieCatalogs {

  private static final Cache<String, NessieCatalog> CATALOG_CACHE = Caffeine.newBuilder()
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .removalListener(
        (RemovalListener<String, NessieCatalog>) (uri, catalog, cause) -> catalog.close())
      .build();

  private NessieCatalogs() {}

  public static NessieCatalog loadCatalog(Configuration conf) {
    // metastore URI can be null in local mode
    String metastoreUri = conf.get("nessie.url", "");
    return CATALOG_CACHE.get(metastoreUri, uri -> new NessieCatalog(conf));
  }
}
