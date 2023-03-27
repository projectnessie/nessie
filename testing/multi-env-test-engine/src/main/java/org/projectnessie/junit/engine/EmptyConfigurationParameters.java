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
package org.projectnessie.junit.engine;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.junit.platform.engine.ConfigurationParameters;

public class EmptyConfigurationParameters implements ConfigurationParameters {
  @Override
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public int size() {
    return 0;
  }

  @Override
  public Optional<String> get(String key) {
    return Optional.empty();
  }

  @Override
  public Optional<Boolean> getBoolean(String key) {
    return Optional.empty();
  }

  @Override
  public Set<String> keySet() {
    return Collections.emptySet();
  }
}
