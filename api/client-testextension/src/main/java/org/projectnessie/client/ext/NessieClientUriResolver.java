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
package org.projectnessie.client.ext;

import java.net.URI;

/**
 * This interface can be implemented by test classes to customize the resolution of Nessie Client
 * URIs.
 */
@FunctionalInterface
public interface NessieClientUriResolver {
  /**
   * Resolved the base Web Container URI to a version-specific Nessie REST API URI.
   *
   * @param apiVersion Requested Nessie API version.
   * @param base Web Container URI without Nessie API-specific path elements.
   * @return version-specific Nessie REST API URI.
   */
  URI resolve(NessieApiVersion apiVersion, URI base);
}
