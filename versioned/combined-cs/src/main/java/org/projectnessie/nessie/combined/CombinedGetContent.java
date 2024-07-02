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
package org.projectnessie.nessie.combined;

import java.util.Map;
import org.projectnessie.api.v2.TreeApi;
import org.projectnessie.client.builder.BaseGetContentBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.Reference;

final class CombinedGetContent extends BaseGetContentBuilder {
  private final TreeApi treeApi;

  CombinedGetContent(TreeApi treeApi) {
    this.treeApi = treeApi;
  }

  @Override
  public Map<ContentKey, Content> get() throws NessieNotFoundException {
    return getWithResponse().toContentsMap();
  }

  @Override
  public ContentResponse getSingle(ContentKey key) throws NessieNotFoundException {
    if (!request.build().getRequestedKeys().isEmpty()) {
      throw new IllegalStateException(
          "Must not use getSingle() with key() or keys(), pass the single key to getSingle()");
    }
    try {
      String ref = Reference.toPathString(refName, hashOnRef);
      if (ref.isEmpty()) {
        ref = "-";
      }
      return treeApi.getContent(key, ref, false, forWrite);
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }

  @Override
  public GetMultipleContentsResponse getWithResponse() throws NessieNotFoundException {
    try {
      String ref = Reference.toPathString(refName, hashOnRef);
      if (ref.isEmpty()) {
        // This is rather a hack to make
        // org.projectnessie.jaxrs.tests.BaseTestNessieApi.contentsOnDefaultBranch[V1]
        // pass.
        ref = "-";
      }
      return treeApi.getMultipleContents(ref, request.build(), false, forWrite);
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
