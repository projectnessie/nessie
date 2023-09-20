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
package org.projectnessie.client.rest.v1;

import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.builder.BaseAssignReferenceBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.Tag;

final class HttpAssignTag extends BaseAssignReferenceBuilder<AssignTagBuilder>
    implements AssignTagBuilder {

  private final NessieApiClient client;

  HttpAssignTag(NessieApiClient client) {
    this.client = client;
    refType(ReferenceType.TAG);
  }

  @Override
  public AssignTagBuilder tagName(String tagName) {
    return refName(tagName);
  }

  @Override
  public void assign() throws NessieNotFoundException, NessieConflictException {
    client.getTreeApi().assignReference(ReferenceType.TAG, refName, expectedHash, assignTo);
  }

  @Override
  public Tag assignAndGet() {
    throw new UnsupportedOperationException(
        "The assignAndGet operation is not supported for tags in API v1");
  }
}
