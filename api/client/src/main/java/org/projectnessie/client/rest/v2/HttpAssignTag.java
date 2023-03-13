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

import org.projectnessie.client.api.AssignTagBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.Tag;

final class HttpAssignTag extends BaseHttpAssignReference<Tag, AssignTagBuilder>
    implements AssignTagBuilder {

  HttpAssignTag(HttpClient client) {
    super(client);
    refType(ReferenceType.TAG);
  }

  @Override
  public AssignTagBuilder tagName(String tagName) {
    return refName(tagName);
  }
}
