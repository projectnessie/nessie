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

import java.io.Closeable;
import java.util.Optional;
import org.projectnessie.api.v1.http.HttpConfigApi;
import org.projectnessie.api.v1.http.HttpContentApi;
import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.http.HttpNamespaceApi;
import org.projectnessie.api.v1.http.HttpTreeApi;
import org.projectnessie.client.http.HttpClient;

public class NessieApiClient implements Closeable {
  private final HttpConfigApi config;
  private final HttpTreeApi tree;
  private final HttpContentApi content;
  private final HttpDiffApi diff;
  private final HttpNamespaceApi namespace;

  public NessieApiClient(
      HttpConfigApi config,
      HttpTreeApi tree,
      HttpContentApi content,
      HttpDiffApi diff,
      HttpNamespaceApi namespace) {
    this.config = config;
    this.tree = tree;
    this.content = content;
    this.diff = diff;
    this.namespace = namespace;
  }

  public Optional<HttpClient> httpClient() {
    return Optional.empty();
  }

  public HttpTreeApi getTreeApi() {
    return tree;
  }

  public HttpContentApi getContentApi() {
    return content;
  }

  public HttpConfigApi getConfigApi() {
    return config;
  }

  public HttpDiffApi getDiffApi() {
    return diff;
  }

  public HttpNamespaceApi getNamespaceApi() {
    return namespace;
  }

  @Override
  public void close() {}
}
