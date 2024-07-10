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
package org.projectnessie.catalog.files.gcs;

import com.google.cloud.BaseServiceException;
import org.projectnessie.catalog.files.api.BackendErrorStatus;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;

/** Extracts storage-side HTTP status code from GCS client exceptions. */
public final class GcsExceptionMapper implements BackendExceptionMapper.Analyzer {

  public static final GcsExceptionMapper INSTANCE = new GcsExceptionMapper();

  private GcsExceptionMapper() {}

  @Override
  public BackendErrorStatus analyze(Throwable th) {
    if (th instanceof BaseServiceException) {
      return BackendErrorStatus.fromHttpStatusCode(((BaseServiceException) th).getCode(), th);
    }
    return null;
  }
}
