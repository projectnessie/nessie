/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.rest.v2;

import org.projectnessie.client.api.UpdateRepositoryConfigBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.model.ImmutableUpdateRepositoryConfigRequest;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.UpdateRepositoryConfigRequest;
import org.projectnessie.model.UpdateRepositoryConfigResponse;

final class HttpUpdateRepositoryConfig implements UpdateRepositoryConfigBuilder {
  private final HttpClient client;
  private RepositoryConfig update;

  HttpUpdateRepositoryConfig(HttpClient client) {
    this.client = client;
  }

  @Override
  public UpdateRepositoryConfigBuilder repositoryConfig(RepositoryConfig update) {
    if (this.update != null) {
      throw new IllegalStateException("repository config to update has already been set");
    }
    this.update = update;
    return this;
  }

  @Override
  public UpdateRepositoryConfigResponse update() throws NessieConflictException {
    if (this.update == null) {
      throw new IllegalStateException("repository config to update must be set");
    }
    UpdateRepositoryConfigRequest req =
        ImmutableUpdateRepositoryConfigRequest.builder().config(update).build();
    return client
        .newRequest()
        .path("config/repository")
        .unwrap(NessieConflictException.class)
        .post(req)
        .readEntity(UpdateRepositoryConfigResponse.class);
  }
}
