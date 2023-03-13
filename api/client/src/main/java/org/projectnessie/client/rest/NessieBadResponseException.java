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
package org.projectnessie.client.rest;

import org.projectnessie.error.NessieError;

/**
 * Represents an unexpected response from the remote side, trying to access a Nessie endpoint. For
 * example, non-JSON responses cause this unchecked exception to be thrown. Note: this exception is
 * <em>not</em> thrown for an HTTP/400 (Bad Request).
 */
public class NessieBadResponseException extends NessieServiceException {

  public NessieBadResponseException(NessieError serverError) {
    super(serverError);
  }
}
