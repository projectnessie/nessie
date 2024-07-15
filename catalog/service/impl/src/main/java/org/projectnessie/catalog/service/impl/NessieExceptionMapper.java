/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.impl;

import org.projectnessie.catalog.files.api.BackendErrorCode;
import org.projectnessie.catalog.files.api.BackendErrorStatus;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.error.BaseNessieClientServerException;

public class NessieExceptionMapper implements BackendExceptionMapper.Analyzer {
  public static final NessieExceptionMapper INSTANCE = new NessieExceptionMapper();

  private NessieExceptionMapper() {}

  @Override
  public BackendErrorStatus analyze(Throwable th) {
    if (th instanceof BaseNessieClientServerException) {
      return BackendErrorStatus.of(BackendErrorCode.NESSIE_ERROR, th);
    }
    return null;
  }
}
