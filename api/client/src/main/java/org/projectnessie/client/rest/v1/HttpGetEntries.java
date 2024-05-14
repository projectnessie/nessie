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

import java.util.Collection;
import org.projectnessie.api.v1.params.EntriesParams;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.builder.BaseGetEntriesBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;

final class HttpGetEntries extends BaseGetEntriesBuilder<EntriesParams> {

  private final NessieApiClient client;
  private Integer namespaceDepth;

  HttpGetEntries(NessieApiClient client) {
    super(EntriesParams::forNextPage);
    this.client = client;
  }

  @Override
  @Deprecated
  public GetEntriesBuilder namespaceDepth(Integer namespaceDepth) {
    this.namespaceDepth = namespaceDepth;
    return this;
  }

  @Override
  protected EntriesParams params() {
    return EntriesParams.builder()
        .namespaceDepth(namespaceDepth)
        .filter(filter)
        .maxRecords(maxRecords)
        .hashOnRef(hashOnRef)
        .build();
  }

  @Override
  public HttpGetEntries key(ContentKey key) {
    throw new UnsupportedOperationException(
        "Requesting individual keys is not supported in API v1.");
  }

  @Override
  public HttpGetEntries keys(Collection<ContentKey> keys) {
    throw new UnsupportedOperationException(
        "Requesting individual keys is not supported in API v1.");
  }

  @Override
  public HttpGetEntries minKey(ContentKey minKey) {
    throw new UnsupportedOperationException("Requesting key ranges is not supported in API v1.");
  }

  @Override
  public HttpGetEntries maxKey(ContentKey maxKey) {
    throw new UnsupportedOperationException("Requesting key ranges is not supported in API v1.");
  }

  @Override
  public GetEntriesBuilder prefixKey(ContentKey prefixKey) {
    throw new UnsupportedOperationException("Requesting key ranges is not supported in API v1.");
  }

  @Override
  protected EntriesResponse get(EntriesParams p) throws NessieNotFoundException {
    if (withContent) {
      throw new IllegalArgumentException("'withContent' is not available with REST API v1");
    }
    return client.getTreeApi().getEntries(refName, p);
  }
}
