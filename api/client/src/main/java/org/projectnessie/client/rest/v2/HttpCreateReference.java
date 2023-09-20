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
package org.projectnessie.client.rest.v2;

import org.projectnessie.client.builder.BaseCreateReferenceBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SingleReferenceResponse;

public class HttpCreateReference extends BaseCreateReferenceBuilder {

  private final HttpClient client;

  protected HttpCreateReference(HttpClient client) {
    this.client = client;
  }

  @Override
  public Reference create() throws NessieNotFoundException, NessieConflictException {
    Reference source;
    if (sourceRefName == null) {
      if (reference.getHash() == null) {
        source = null;
      } else {
        source = Detached.of(reference.getHash());
      }
    } else {
      source = Branch.of(sourceRefName, reference.getHash());
    }

    return client
        .newRequest()
        .path("trees")
        .queryParam("name", reference.getName())
        .queryParam("type", reference.getType().name())
        .unwrap(NessieNotFoundException.class, NessieConflictException.class)
        .post(source) // TODO: support all types
        .readEntity(SingleReferenceResponse.class)
        .getReference();
  }
}
