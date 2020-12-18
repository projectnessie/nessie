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
package com.dremio.nessie.client;

import javax.ws.rs.client.WebTarget;

import com.dremio.nessie.api.AdminApi;
import com.dremio.nessie.error.NessieUnsupportedOperationException;

/**
 * Administrative interface for Nessie.
 */
class ClientAdminApi implements AdminApi {
  private final WebTarget target;

  ClientAdminApi(WebTarget target) {
    this.target = target;
  }

  @Override
  public void resetStoreUnsafe() throws NessieUnsupportedOperationException {
    target.path("admin/resetStoreUnsafe")
          .request()
          .delete();
  }
}
