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

import java.util.Objects;
import org.projectnessie.api.http.HttpApiUtil;
import org.projectnessie.client.api.OnMutableReferenceBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.model.MutableReference;

abstract class BaseHttpOnMutableReferenceRequest<R extends OnMutableReferenceBuilder<R>>
    extends BaseHttpRequest implements OnMutableReferenceBuilder<R> {
  protected MutableReference reference;

  BaseHttpOnMutableReferenceRequest(NessieApiClient client) {
    super(client);
  }

  protected MutableReference reference() {
    return Objects.requireNonNull(reference, "Reference not set on request");
  }

  protected String referenceTypeName() {
    return HttpApiUtil.referenceTypeName(reference());
  }

  @Override
  public R reference(MutableReference reference) {
    this.reference = reference;
    return (R) this;
  }
}
