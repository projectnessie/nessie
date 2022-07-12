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

import java.util.stream.Stream;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.api.params.EntriesParamsBuilder;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.http.NessieApiClient;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;

final class HttpGetEntries extends BaseHttpOnReferenceRequest<GetEntriesBuilder>
    implements GetEntriesBuilder {

  private final EntriesParamsBuilder params = EntriesParams.builder();

  HttpGetEntries(NessieApiClient client) {
    super(client);
  }

  @Override
  public GetEntriesBuilder maxRecords(int maxRecords) {
    params.maxRecords(maxRecords);
    return this;
  }

  @Override
  public GetEntriesBuilder pageToken(String pageToken) {
    params.pageToken(pageToken);
    return this;
  }

  @Override
  public GetEntriesBuilder filter(String filter) {
    params.filter(filter);
    return this;
  }

  @Override
  public GetEntriesBuilder namespaceDepth(Integer namespaceDepth) {
    params.namespaceDepth(namespaceDepth);
    return this;
  }

  private EntriesParams params() {
    params.hashOnRef(hashOnRef);
    return params.build();
  }

  @Override
  public EntriesResponse get() throws NessieNotFoundException {
    return get(params());
  }

  private EntriesResponse get(EntriesParams p) throws NessieNotFoundException {
    return client.getTreeApi().getEntries(refName, p);
  }

  @Override
  public Stream<Entry> stream() throws NessieNotFoundException {
    EntriesParams p = params();
    return StreamingUtil.generateStream(
        EntriesResponse::getEntries, pageToken -> get(p.forNextPage(pageToken)));
  }
}
