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

import org.projectnessie.client.api.OnReferenceBuilder;
import org.projectnessie.client.http.NessieApiClient;

abstract class BaseHttpOnReferenceRequest<R extends OnReferenceBuilder<R>> extends BaseHttpRequest
    implements OnReferenceBuilder<R> {
  protected String refName;
  protected String hashOnRef;

  BaseHttpOnReferenceRequest(NessieApiClient client) {
    super(client);
  }

  @Override
  public R refName(String refName) {
    this.refName = refName;
    return (R) this;
  }

  @Override
  public R hashOnRef(String hashOnRef) {
    this.hashOnRef = hashOnRef;
    return (R) this;
  }
}
