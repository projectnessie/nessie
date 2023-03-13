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

import org.projectnessie.client.api.DeleteTagBuilder;
import org.projectnessie.client.builder.BaseOnTagBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

final class HttpDeleteTag extends BaseOnTagBuilder<DeleteTagBuilder> implements DeleteTagBuilder {

  private final NessieApiClient client;

  HttpDeleteTag(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public void delete() throws NessieConflictException, NessieNotFoundException {
    client.getTreeApi().deleteReference(Reference.ReferenceType.TAG, tagName, hash);
  }

  @Override
  public Tag getAndDelete() {
    throw new UnsupportedOperationException(
        "The getAndDelete operation is not supported for tags in API v1");
  }
}
