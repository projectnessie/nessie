/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.quarkus.providers.storage;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.projectnessie.quarkus.config.VersionStoreConfig;

/**
 * This config source factory is used to activate the Quarkus-MongoDB driver when one of the MongoDB
 * Nessie version stores is used, and otherwise disable the Quarkus-MongoDB driver.
 *
 * <p>The Quarkus configuration {@code quarkus.mongodb.active}, defaults to {@code true}, got added
 * via Quarkus 3.31.0.
 *
 * <p>Having a Quarkus-Mongo driver active means that it will be considered during the readiness and
 * health checks. In other words, the default of {@code true} <em>breaks</em> non-MongoDB version
 * store types.
 */
public class MongoDBConfigSourceFactory implements ConfigSourceFactory {
  @Override
  public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
    return List.of(
        new ConfigSource() {
          static final String ACTIVE_PROPERTY = "quarkus.mongodb.active";
          static final Set<String> PROPERTY_NAMES = Set.of(ACTIVE_PROPERTY);

          @SuppressWarnings("deprecation")
          private String activeValue() {
            var storeType = context.getValue("nessie.version.store.type");
            if (storeType == null) {
              return "false";
            }

            return switch (VersionStoreConfig.VersionStoreType.valueOf(
                storeType.getValue().toUpperCase(Locale.ROOT))) {
              case MONGODB, MONGODB2 -> "true";
              default -> "false";
            };
          }

          @Override
          public Map<String, String> getProperties() {
            return Map.of(ACTIVE_PROPERTY, activeValue());
          }

          @Override
          public int getOrdinal() {
            // allows overriding the value in config files, system properties and environment
            // variables
            return 150;
          }

          @Override
          public Set<String> getPropertyNames() {
            return PROPERTY_NAMES;
          }

          @Override
          public String getValue(String propertyName) {
            if (ACTIVE_PROPERTY.equals(propertyName)) {
              return activeValue();
            }
            return null;
          }

          @Override
          public String getName() {
            return "MongoDB-active config provider";
          }
        });
  }
}
