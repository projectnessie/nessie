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

import org.projectnessie.client.auth.NessieAuthentication;

/**
 * A special {@link RequestFilter} that applies authentication to the HTTP request, typically in the
 * form of HTTP headers.
 */
public interface HttpAuthentication extends NessieAuthentication {

  /**
   * Configure the given {@link HttpClient} to use this authentication. Called when authentication
   * is set on the {@link HttpClient} level only.
   *
   * @see HttpClient.Builder#setAuthentication(HttpAuthentication)
   * @param client client to configure
   */
  void applyToHttpClient(HttpClient.Builder client);

  /**
   * Apply authentication to the HTTP request. Called when authentication is set on the {@link
   * HttpRequest} level only.
   *
   * @see HttpRequest#authentication(HttpAuthentication)
   * @param context The request context
   */
  default void applyToHttpRequest(RequestContext context) {
    throw new UnsupportedOperationException(
        "This authentication method does not support per-request authentication");
  }

  @Override
  default HttpAuthentication copy() {
    return this;
  }
}
