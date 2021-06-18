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
package org.projectnessie.common;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/** Helper class for Nessie version related functionality. */
public class NessieVersion {
  public static final Version NESSIE_VERSION;
  public static final Version NESSIE_MIN_API_VERSION;

  static {
    URL nessieInfo = NessieVersion.class.getResource("nessie.properties");
    Properties properties = new Properties();
    try {
      if (nessieInfo == null) {
        throw new Exception("nessie.properties resource not found");
      }
      try (InputStream in = nessieInfo.openConnection().getInputStream()) {
        properties.load(in);

        NESSIE_VERSION = Version.parse(properties.getProperty("nessie.version"));
        NESSIE_MIN_API_VERSION = Version.parse(properties.getProperty("nessie.min-remote-version"));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load nessie.properties", e);
    }
  }

  /**
   * Verifies that the given Nessie version is "API compatible", which means that the given version
   * is greater than or equal to the value of the property {@code nessie.min-remote-version} in the
   * {@code nessie.properties} file.
   *
   * @param otherVersion version to check
   * @return {@code true}, if "API compatible", {@code false} otherwise
   */
  public static boolean isApiCompatible(String otherVersion) {
    return Version.parse(otherVersion).compareTo(NESSIE_MIN_API_VERSION) >= 0;
  }
}
