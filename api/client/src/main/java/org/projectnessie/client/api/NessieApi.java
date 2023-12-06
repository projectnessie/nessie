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
package org.projectnessie.client.api;

import java.util.Optional;

/** Base interface for all Nessie-API versions. */
public interface NessieApi extends AutoCloseable {

  // Overridden to "remove 'throws Exception'"
  @Override
  void close();

  /**
   * Returns the possibly protocol but definitely implementation specific client instance.
   *
   * @param clientType Expected client type.
   * @return an {@link Optional} with either the client of the requested type or empty.
   * @param <C> requested/expected client type.
   */
  default <C> Optional<C> unwrapClient(Class<C> clientType) {
    return Optional.empty();
  }
}
