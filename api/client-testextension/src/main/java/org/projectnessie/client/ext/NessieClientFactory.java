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
package org.projectnessie.client.ext;

import jakarta.annotation.Nonnull;
import org.projectnessie.client.api.NessieApiV1;

/**
 * Interface for declaring parameters in test methods wishing to operate on a Nessie Client
 * pre-configured to talk to the Nessie Server running in the current test environment.
 *
 * <p>An implementation of this interface will be injected into test method parameters by {@link
 * NessieClientResolver}.
 */
public interface NessieClientFactory {

  NessieApiVersion apiVersion();

  @Nonnull
  default NessieApiV1 make() {
    return make((builder, version) -> builder);
  }

  @Nonnull
  NessieApiV1 make(NessieClientCustomizer customizer);
}
