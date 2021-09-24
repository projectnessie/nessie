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
package org.projectnessie.client.api;

import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;

/**
 * Request builder for "get contents".
 *
 * @since {@link NessieApiV1}
 */
public interface GetContentsBuilder extends OnReferenceBuilder<GetContentsBuilder> {
  GetContentsBuilder key(@Valid ContentsKey key);

  GetContentsBuilder keys(List<ContentsKey> keys);

  Map<ContentsKey, Contents> get() throws NessieNotFoundException;
}
