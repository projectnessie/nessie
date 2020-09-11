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

package com.dremio.nessie.client.rest;

import javax.ws.rs.client.ResponseProcessingException;

import com.dremio.nessie.error.NessieError;

public class NessieExtendedClientErrorException extends ResponseProcessingException
    implements NessieServiceException {

  private final NessieError nessieError;

  public NessieExtendedClientErrorException(NessieError nessieError) {
    super(null, null, null);
    this.nessieError = nessieError;
  }

  public NessieError getNessieError() {
    return nessieError;
  }
}
