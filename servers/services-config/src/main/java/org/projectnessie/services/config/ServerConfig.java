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
public interface ServerConfig extends ExceptionConfig {

  int DEFAULT_ACCESS_CHECK_BATCH_SIZE = 100;

  /**
   * The default branch to use if not provided by the user.
   *
   * @return the branch to use
   */
  String getDefaultBranch();

  /**
   * The number of entity-checks that are grouped into a call to `BatchAccessChecker`. The default
   * value is quite conservative, it is the responsibility of the operator to adjust this value
   * according to the capabilities of the actual authz implementation. Note that the number of
   * checks can be slightly exceeded by the implementation, depending on the call site.
   */
  default int accessChecksBatchSize() {
    return DEFAULT_ACCESS_CHECK_BATCH_SIZE;
  }
}
