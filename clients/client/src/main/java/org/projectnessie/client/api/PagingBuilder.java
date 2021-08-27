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

import org.projectnessie.model.EntriesResponse;

public interface PagingBuilder<R extends PagingBuilder<R>> extends OnReferenceBuilder<R> {

  /** Recommended: specify the maximum number of records to return. */
  R maxRecords(int maxRecords);

  /**
   * For paged requests: pass the {@link EntriesResponse#getToken()} from the previous request,
   * otherwise don't call this method.
   */
  R pageToken(String pageToken);

  /** Required: the <a href="https://github.com/projectnessie/cel-java">CEL</a> script. */
  R queryExpression(String queryExpression);
}
