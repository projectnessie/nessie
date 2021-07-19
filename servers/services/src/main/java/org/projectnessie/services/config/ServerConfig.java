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
package org.projectnessie.services.config;

/** Nessie server configuration to be injected into the JAX-RS application. */
public interface ServerConfig {

  /**
   * Gets the branch to use if not provided by the user.
   *
   * @return the branch to use
   */
  String getDefaultBranch();

  /**
   * Returns {@code true} if server stack trace should be sent to the client in case of error.
   *
   * @return {@code true} if the server should send the stack trace to the client.
   */
  boolean sendStacktraceToClient();
}
