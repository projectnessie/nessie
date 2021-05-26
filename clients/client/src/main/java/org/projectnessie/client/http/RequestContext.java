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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import org.projectnessie.client.http.HttpClient.Method;

/** Context containing all important info about a request. */
public class RequestContext {

  private final Map<String, Set<String>> headers;
  private final URI uri;
  private final Method method;
  private final Object body;
  private List<BiConsumer<ResponseContext, Exception>> responseCallbacks;

  /**
   * Construct a request context.
   *
   * @param headers map of all headers
   * @param uri uri of the request
   * @param method verb to be used
   * @param body optional body of request
   */
  public RequestContext(Map<String, Set<String>> headers, URI uri, Method method, Object body) {
    this.headers = headers;
    this.uri = uri;
    this.method = method;
    this.body = body;
  }

  public Map<String, Set<String>> getHeaders() {
    return headers;
  }

  public void putHeader(String name, String value) {
    HttpRequest.putHeader(name, value, headers);
  }

  public URI getUri() {
    return uri;
  }

  public Method getMethod() {
    return method;
  }

  public Optional<Object> getBody() {
    return body == null ? Optional.empty() : Optional.of(body);
  }

  /**
   * Adds a callback to be called when the request has finished. The {@code responseCallback} {@link
   * BiConsumer consumer} is called with a non-{@code null} {@link ResponseContext}, if the HTTP
   * request technically succeeded. The The {@code responseCallback} {@link BiConsumer consumer} is
   * called with a non-{@code null} {@link Exception} object, if the HTTP request technically
   * failed.
   *
   * @param responseCallback callback that receives either a non-{@code null} {@link
   *     ResponseContext} or a non-{@code null} {@link Exception}.
   */
  public void addResponseCallback(BiConsumer<ResponseContext, Exception> responseCallback) {
    if (responseCallbacks == null) {
      responseCallbacks = new ArrayList<>();
    }
    responseCallbacks.add(responseCallback);
  }

  List<BiConsumer<ResponseContext, Exception>> getResponseCallbacks() {
    return responseCallbacks;
  }
}
