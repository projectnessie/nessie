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
import org.projectnessie.client.api.GetContentsBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.ImmutableMultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;

final class HttpGetContents extends BaseHttpOnReferenceRequest<GetContentsBuilder>
    implements GetContentsBuilder {

  private final ImmutableMultiGetContentsRequest.Builder request =
      ImmutableMultiGetContentsRequest.builder();

  HttpGetContents(NessieApiClient client) {
    super(client);
  }

  @Override
  public GetContentsBuilder key(ContentsKey key) {
    request.addRequestedKeys(key);
    return this;
  }

  @Override
  public GetContentsBuilder keys(List<ContentsKey> keys) {
    request.addAllRequestedKeys(keys);
    return this;
  }

  @Override
  public Map<ContentsKey, Contents> get() throws NessieNotFoundException {
    MultiGetContentsResponse resp =
        client.getContentsApi().getMultipleContents(refName, hashOnRef, request.build());
    return resp.getContents().stream()
        .collect(Collectors.toMap(ContentsWithKey::getKey, ContentsWithKey::getContents));
  }
}
