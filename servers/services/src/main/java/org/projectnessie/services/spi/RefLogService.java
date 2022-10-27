/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.services.spi;

import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;

/**
 * Server-side interface to services managing the reflog.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface RefLogService {
  RefLogResponse getRefLog(
      String startHash, String endHash, String filter, Integer maxRecords, String pageToken)
      throws NessieNotFoundException;
}
