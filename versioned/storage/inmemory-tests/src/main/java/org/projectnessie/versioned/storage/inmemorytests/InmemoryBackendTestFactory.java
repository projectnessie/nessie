/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.inmemorytests;

import java.util.Map;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackend;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;

public class InmemoryBackendTestFactory implements BackendTestFactory {

  @Override
  public InmemoryBackend createNewBackend() {
    return new InmemoryBackend();
  }

  @Override
  public String getName() {
    return InmemoryBackendFactory.NAME;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of();
  }
}
