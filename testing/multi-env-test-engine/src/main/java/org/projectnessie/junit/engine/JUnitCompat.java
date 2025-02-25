/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.junit.engine;

import org.junit.jupiter.engine.config.DefaultJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.reporting.OutputDirectoryProvider;

final class JUnitCompat {
  private JUnitCompat() {}

  static Class<?> CLASS_OUTPUT_DIRECTORY_PROVIDER;

  static {
    try {
      CLASS_OUTPUT_DIRECTORY_PROVIDER =
          Class.forName("org.junit.platform.engine.reporting.OutputDirectoryProvider");
    } catch (ClassNotFoundException e) {
      CLASS_OUTPUT_DIRECTORY_PROVIDER = null;
    }
  }

  static JupiterConfiguration newDefaultJupiterConfiguration(EngineDiscoveryRequest request) {
    return newDefaultJupiterConfiguration(request.getConfigurationParameters(), request);
  }

  static JupiterConfiguration newDefaultJupiterConfiguration(
      ConfigurationParameters configurationParameters, EngineDiscoveryRequest request) {
    try {
      if (CLASS_OUTPUT_DIRECTORY_PROVIDER != null) {
        return newDefaultJupiterConfiguration512(configurationParameters, request);
      }

      var ctorBefore512 =
          DefaultJupiterConfiguration.class.getDeclaredConstructor(ConfigurationParameters.class);
      return ctorBefore512.newInstance(configurationParameters);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static JupiterConfiguration newDefaultJupiterConfiguration512(
      ConfigurationParameters configurationParameters, EngineDiscoveryRequest request)
      throws Exception {
    var ctorSince512 =
        DefaultJupiterConfiguration.class.getDeclaredConstructor(
            ConfigurationParameters.class, OutputDirectoryProvider.class);
    return ctorSince512.newInstance(configurationParameters, request.getOutputDirectoryProvider());
  }
}
