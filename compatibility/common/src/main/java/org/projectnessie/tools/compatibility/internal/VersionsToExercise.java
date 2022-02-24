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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.projectnessie.tools.compatibility.api.Version;

final class VersionsToExercise {

  public static final String NESSIE_VERSIONS_PROPERTY = "nessie.versions";

  private VersionsToExercise() {}

  static SortedSet<Version> versionsFromValue(String value) {
    TreeSet<Version> versions =
        Stream.of(value)
            .filter(Objects::nonNull)
            .flatMap(v -> Arrays.stream(v.split(",")))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(Version::parseVersion)
            .collect(Collectors.toCollection(TreeSet::new));
    if (versions.isEmpty()) {
      throw new IllegalArgumentException("No versions to test");
    }
    return versions;
  }

  static String valueFromResource(String propertyName) {
    URL res =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("META-INF/nessie-compatibility.properties");
    if (res != null) {
      try (InputStream in = res.openConnection().getInputStream()) {
        Properties props = new Properties();
        props.load(in);
        return props.getProperty(propertyName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  public static SortedSet<Version> versionsForEngine(
      EngineDiscoveryRequest discoveryRequest, String propertyName) {
    String value = discoveryRequest.getConfigurationParameters().get(propertyName).orElse(null);
    if (value == null) {
      value = System.getProperty(propertyName);
    }
    if (value == null) {
      value = valueFromResource(propertyName);
    }
    if (value == null) {
      throw new IllegalStateException(String.format("Property '%s' not defined", propertyName));
    }
    return versionsFromValue(value);
  }

  public static SortedSet<Version> versionsForEngine(EngineDiscoveryRequest discoveryRequest) {
    return versionsForEngine(discoveryRequest, NESSIE_VERSIONS_PROPERTY);
  }
}
