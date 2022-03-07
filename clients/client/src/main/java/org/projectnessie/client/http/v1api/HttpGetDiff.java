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
package org.projectnessie.client.http.v1api;

import org.projectnessie.api.params.DiffParams;
import org.projectnessie.api.params.DiffParamsBuilder;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;

final class HttpGetDiff extends BaseHttpRequest implements GetDiffBuilder {

  DiffParamsBuilder builder = DiffParams.builder();

  HttpGetDiff(NessieApiClient client) {
    super(client);
  }

  @Override
  public GetDiffBuilder fromRefName(String fromRefName) {
    builder.fromRef(fromRefName);
    return this;
  }

  @Override
  public GetDiffBuilder fromHashOnRef(String fromHashOnRef) {
    builder.fromHashOnRef(fromHashOnRef);
    return this;
  }

  @Override
  public GetDiffBuilder toRefName(String toRefName) {
    builder.toRef(toRefName);
    return this;
  }

  @Override
  public GetDiffBuilder toHashOnRef(String toHashOnRef) {
    builder.toHashOnRef(toHashOnRef);
    return this;
  }

  @Override
  public DiffResponse get() throws NessieNotFoundException {
    return client.getDiffApi().getDiff(builder.build());
  }
}
