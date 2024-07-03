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

import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.builder.BaseGetContentBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;

final class HttpGetContent extends BaseGetContentBuilder {

  private final NessieApiClient client;

  HttpGetContent(NessieApiClient client) {
    this.client = client;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    GetMultipleContentsResponse resp =
        client.getContentApi().getMultipleContents(refName, hashOnRef, request.build());
    return resp.getContents().stream()
        .collect(Collectors.toMap(ContentWithKey::getKey, ContentWithKey::getContent));
  }

  @Override
  public ContentResponse getSingle(ContentKey key) {
    throw new UnsupportedOperationException("Get single content is not available in API v1");
  }

  @Override
  public GetMultipleContentsResponse getWithResponse() {
    throw new UnsupportedOperationException(
        "Extended contents response data is not available in API v1");
  }

  @Override
  public GetContentBuilder forWrite(boolean forWrite) {
    if (forWrite) {
      throw new UnsupportedOperationException("forWrite is not available in API v1");
    }
    return super.forWrite(forWrite);
  }
}
