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
package org.projectnessie.api;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.projectnessie.api.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

public interface DiffApi {

  /**
   * Returns a list of diff values that show the difference between two given references.
   *
   * @param params The {@link DiffParams} that includes the parameters for this API call.
   * @return A list of diff values that show the difference between two given references.
   */
  DiffResponse getDiff(@Valid @NotNull DiffParams params) throws NessieNotFoundException;
}
