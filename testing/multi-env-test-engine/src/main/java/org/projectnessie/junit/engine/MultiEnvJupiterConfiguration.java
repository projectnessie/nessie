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
package org.projectnessie.junit.engine;

import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.engine.config.DefaultJupiterConfiguration;
import org.junit.platform.engine.ConfigurationParameters;

public class MultiEnvJupiterConfiguration extends DefaultJupiterConfiguration {

  private final String environmentIds;

  public MultiEnvJupiterConfiguration(
      ConfigurationParameters configurationParameters, String environmentIds) {
    super(configurationParameters);
    this.environmentIds = environmentIds;
  }

  @Override
  public DisplayNameGenerator getDefaultDisplayNameGenerator() {
    DisplayNameGenerator delegate = super.getDefaultDisplayNameGenerator();
    return new MultiEnvDisplayNameGenerator(delegate, environmentIds);
  }
}
