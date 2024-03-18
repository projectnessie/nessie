/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.client.http.impl.jdk8;

import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.impl.HttpClientFactory;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

public class UrlConnectionClientFactory implements HttpClientFactory {
  @Override
  public HttpClient buildClient(HttpRuntimeConfig config) {
    return new UrlConnectionClient(config);
  }

  @Override
  public boolean available() {
    return true;
  }

  @Override
  public int priority() {
    return 90;
  }

  @Override
  public String name() {
    return "URLConnection";
  }
}
