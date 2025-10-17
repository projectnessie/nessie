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

@SuppressWarnings("deprecation")
final class JUnitCompat {
  private JUnitCompat() {}

  static Class<?> CLASS_OUTPUT_DIRECTORY_CREATOR;

  static {
    try {
      CLASS_OUTPUT_DIRECTORY_CREATOR =
          Class.forName("org.junit.platform.engine.OutputDirectoryCreator");
    } catch (ClassNotFoundException e) {
      CLASS_OUTPUT_DIRECTORY_CREATOR = null;
    }
  }

  static JupiterConfiguration newDefaultJupiterConfiguration(EngineDiscoveryRequest request) {
    return newDefaultJupiterConfiguration(request.getConfigurationParameters(), request);
  }

  static JupiterConfiguration newDefaultJupiterConfiguration(
      ConfigurationParameters configurationParameters, EngineDiscoveryRequest request) {
    try {
      if (CLASS_OUTPUT_DIRECTORY_CREATOR != null) {
        return newDefaultJupiterConfiguration514(configurationParameters, request);
      }

      var ctorBefore514 =
          DefaultJupiterConfiguration.class.getDeclaredConstructor(
              ConfigurationParameters.class, OutputDirectoryProvider.class);
      return ctorBefore514.newInstance(
          configurationParameters, request.getOutputDirectoryProvider());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static JupiterConfiguration newDefaultJupiterConfiguration514(
      ConfigurationParameters configurationParameters, EngineDiscoveryRequest request)
      throws Exception {
    return new DefaultJupiterConfiguration(
        configurationParameters, request.getOutputDirectoryCreator());
  }
}
