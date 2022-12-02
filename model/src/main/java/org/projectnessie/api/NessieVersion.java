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
package org.projectnessie.api;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public final class NessieVersion {
  private NessieVersion() {}

  public static final String NESSIE_VERSION;

  static {
    URL nessieVersionResource = NessieVersion.class.getResource("nessie-version.properties");
    try (InputStream in =
        requireNonNull(
                nessieVersionResource,
                "Resource org/projectnessie/client/nessie-version.properties containing Nessie version does not exist")
            .openConnection()
            .getInputStream()) {
      Properties p = new Properties();
      p.load(in);
      NESSIE_VERSION = p.getProperty("nessie.version");
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to load Nessie version from org/projectnessie/client/nessie-version.properties",
          e);
    }
  }
}
