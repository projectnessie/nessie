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
package org.projectnessie.client.http;

import java.io.Closeable;
import org.projectnessie.api.http.HttpConfigApi;
import org.projectnessie.api.http.HttpContentsApi;
import org.projectnessie.api.http.HttpTreeApi;

public class NessieApiClient implements Closeable {
  private final HttpConfigApi config;
  private final HttpTreeApi tree;
  private final HttpContentsApi contents;

  public NessieApiClient(HttpConfigApi config, HttpTreeApi tree, HttpContentsApi contents) {
    this.config = config;
    this.tree = tree;
    this.contents = contents;
  }

  public HttpTreeApi getTreeApi() {
    return tree;
  }

  public HttpContentsApi getContentsApi() {
    return contents;
  }

  public HttpConfigApi getConfigApi() {
    return config;
  }

  @Override
  public void close() {}
}
