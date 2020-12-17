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
package com.dremio.nessie.client.http;

import java.util.Map;
import java.util.Optional;

import com.dremio.nessie.client.http.HttpClient.Method;

/**
 * Context containing all important info about a request.
 */
public class RequestContext {

  private final Map<String, String> headers;
  private final String uri;
  private final Method method;
  private final Object body;

  /**
   * Construct a request context.
   *
   * @param headers map of all headers
   * @param uri uri of the request
   * @param method verb to be used
   * @param body optional body of request
   */
  public RequestContext(Map<String, String> headers, String uri, Method method, Object body) {
    this.headers = headers;
    this.uri = uri;
    this.method = method;
    this.body = body;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getUri() {
    return uri;
  }

  public Method getMethod() {
    return method;
  }

  public Optional<Object> getBody() {
    return body == null ? Optional.empty() : Optional.of(body);
  }
}
