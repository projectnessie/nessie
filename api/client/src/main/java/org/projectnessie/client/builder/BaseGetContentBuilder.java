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
package org.projectnessie.client.builder;

import java.util.List;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableGetMultipleContentsRequest;

public abstract class BaseGetContentBuilder extends BaseOnReferenceBuilder<GetContentBuilder>
    implements GetContentBuilder {

  protected final ImmutableGetMultipleContentsRequest.Builder request =
      ImmutableGetMultipleContentsRequest.builder();

  protected boolean forWrite;

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
  public GetContentBuilder forWrite(boolean forWrite) {
    this.forWrite = forWrite;
    return this;
  }
}
