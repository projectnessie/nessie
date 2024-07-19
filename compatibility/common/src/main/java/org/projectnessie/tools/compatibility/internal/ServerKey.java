/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.internal;

import static java.util.Objects.requireNonNull;
import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.api.Version;

/** Value object representing the version, type and configuration of a {@link NessieApi}. */
final class ServerKey {
  private final Version version;
  private final String storageName;
  private final Map<String, String> config;

  ServerKey(Version version, String storageName, Map<String, String> config) {
    this.version = requireNonNull(version, "version");
    this.storageName = requireNonNull(storageName, "storageName");
    this.config = requireNonNull(config, "config");
  }

  public static ServerKey forContext(
      ExtensionContext context,
      Version version,
      String storageName,
      Map<String, String> defaultConfig) {
    Map<String, String> config = new HashMap<>(defaultConfig);
    Util.forEachContextFromRoot(
        context,
        c ->
            c.getTestClass()
                .ifPresent(
                    instance ->
                        findRepeatableAnnotations(instance, NessieServerProperty.class)
                            .forEach(prop -> config.put(prop.name(), prop.value()))));

    return new ServerKey(version, storageName, config);
  }

  Version getVersion() {
    return version;
  }

  String getStorageName() {
    return storageName;
  }

  Map<String, String> getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerKey serverKey = (ServerKey) o;
    return Objects.equals(version, serverKey.version)
        && Objects.equals(storageName, serverKey.storageName)
        && Objects.equals(config, serverKey.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, storageName, config);
  }

  @Override
  public String toString() {
    return String.format(
        "server-%s-%s-%s",
        getVersion(),
        storageName,
        config.entrySet().stream()
            .map(e -> e.getKey() + '=' + e.getValue())
            .collect(Collectors.joining("_")));
  }
}
