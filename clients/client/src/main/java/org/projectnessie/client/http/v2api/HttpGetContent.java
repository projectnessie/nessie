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
package org.projectnessie.client.http.v2api;

import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.client.builder.BaseGetContentBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.Reference;

final class HttpGetContent extends BaseGetContentBuilder {
  private final HttpClient client;

  HttpGetContent(HttpClient client) {
    this.client = client;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    GetMultipleContentsResponse response =
        client
            .newRequest()
            .path("trees/{ref}/contents")
            .resolveTemplate("ref", Reference.toPathString(refName, hashOnRef))
            .unwrap(NessieNotFoundException.class)
            .post(request.build())
            .readEntity(GetMultipleContentsResponse.class);
    return response.getContents().stream()
        .collect(Collectors.toMap(ContentWithKey::getKey, ContentWithKey::getContent));
  }
}
