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

package com.dremio.nessie.backend;


import com.dremio.nessie.model.BranchControllerObject;
import com.dremio.nessie.model.BranchControllerReference;
import com.dremio.nessie.model.User;
import com.dremio.nessie.server.ServerConfiguration;

/**
 * Backend for all interactions between database and the Nessie Server.
 *
 * <p>
 *   This backend is intended to be only for the interaction between server and
 *   database. Each EntityBackend is responsible for one type of data object. The
 *   {@link com.dremio.nessie.backend.Backend} object is designed to be easily replaceable
 *   as long as the backing database follows the contract defined in
 *   {@link com.dremio.nessie.backend.EntityBackend}. Each implementation of Backend should provide
 *   an implementation of Factory called BackendFactory for instantiation by the DI framework.
 * </p>
 */
public interface Backend extends AutoCloseable {

  EntityBackend<User> userBackend();

  EntityBackend<BranchControllerObject> gitBackend();

  EntityBackend<BranchControllerReference> gitRefBackend();

  /**
   * factory interface to create a backend. All backends must have a Factory called BackendFactory
   */
  interface Factory {

    Backend create(ServerConfiguration config);
  }
}
