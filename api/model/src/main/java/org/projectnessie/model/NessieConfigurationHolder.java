/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Loads the externally defined {@link NessieConfiguration template} defining the min and max API
 * version ordinals and the specification version string.
 *
 * <p>The {@code org/projectnessie/model/NessieConfiguration.json} resource can be read by external
 * consumers, too, if necessary.
 */
final class NessieConfigurationHolder {
  private NessieConfigurationHolder() {}

  static final NessieConfiguration NESSIE_API_SPEC;

  static {
    try {
      URL src = NessieConfiguration.class.getResource("NessieConfiguration.json");
      URLConnection conn =
          requireNonNull(src, "NessieConfiguration.json not found").openConnection();
      try (var input = conn.getInputStream()) {
        NESSIE_API_SPEC = new ObjectMapper().readValue(input, NessieConfiguration.class);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
