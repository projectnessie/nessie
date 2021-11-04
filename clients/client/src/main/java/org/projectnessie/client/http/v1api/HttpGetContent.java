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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;

final class HttpGetContent extends BaseHttpOnReferenceRequest<GetContentBuilder>
    implements GetContentBuilder {

  private final ImmutableGetMultipleContentsRequest.Builder request =
      ImmutableGetMultipleContentsRequest.builder();

  HttpGetContent(NessieApiClient client) {
    super(client);
  }

  @Override
  public GetContentBuilder key(ContentKey key) {
    request.addRequestedKeys(key);
    return this;
  }

  @Override
  public GetContentBuilder keys(List<ContentKey> keys) {
    request.addAllRequestedKeys(keys);
    return this;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    GetMultipleContentsResponse resp =
        client.getContentApi().getMultipleContents(refName, hashOnRef, request.build());
    return resp.getContents().stream()
        .collect(Collectors.toMap(ContentWithKey::getKey, ContentWithKey::getContent));
  }
}
