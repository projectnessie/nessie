/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;

public class Catalogs implements AutoCloseable {
  private final Map<Map<String, String>, RESTCatalog> catalogs = new HashMap<>();

  public RESTCatalog getCatalog(Map<String, String> options) {
    // normalize
    options = new TreeMap<>(options);

    return catalogs.computeIfAbsent(
        options,
        opts -> {
          int catalogServerPort = Integer.getInteger("quarkus.http.port");
          RESTCatalog c =
              new RESTCatalog(
                  config -> {
                    var builder = HTTPClient.builder(config).uri(config.get(CatalogProperties.URI));
                    config.entrySet().stream()
                        .filter(e -> e.getKey().startsWith("header."))
                        .forEach(
                            e ->
                                builder.withHeader(
                                    e.getKey().substring("header.".length()), e.getValue()));
                    return builder.build();
                  });
          c.setConf(new Configuration());
          Map<String, String> catalogOptions = new HashMap<>();
          catalogOptions.put(
              CatalogProperties.URI,
              String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort));
          catalogOptions.putAll(opts);
          c.initialize(getClass().getSimpleName(), catalogOptions);
          return c;
        });
  }

  @Override
  public void close() throws Exception {
    try {
      for (RESTCatalog catalog : catalogs.values()) {
        catalog.close();
      }
    } finally {
      catalogs.clear();
    }
  }
}
