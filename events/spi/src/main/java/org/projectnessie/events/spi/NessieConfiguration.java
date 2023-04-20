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
package org.projectnessie.events.spi;

import java.util.Map;
import org.immutables.value.Value;

/** Configuration of the Nessie server. */
@Value.Immutable
public interface NessieConfiguration {

  /** Semver version representing the behavior of the Nessie server. */
  String getSpecVersion();

  /**
   * The minimum API version supported by the server.
   *
   * <p>API versions are numbered sequentially, as they are developed.
   */
  int getMinSupportedApiVersion();

  /**
   * The maximum API version supported by the server.
   *
   * <p>API versions are numbered sequentially, as they are developed.
   */
  int getMaxSupportedApiVersion();

  /** Additional configuration properties. Currently, always empty. */
  Map<String, String> getAdditionalProperties();
}
