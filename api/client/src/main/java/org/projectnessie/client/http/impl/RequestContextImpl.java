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
package org.projectnessie.client.http.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;

public class RequestContextImpl implements RequestContext {

  private final HttpHeaders headers;
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
  public RequestContextImpl(HttpHeaders headers, URI uri, Method method, Object body) {
    this.headers = headers;
    this.uri = uri;
    this.method = method;
    this.body = body;
  }

  @Override
  public void putHeader(String name, String value) {
    headers.put(name, value);
  }

  @Override
  public boolean containsHeader(String name) {
    return headers.contains(name);
  }

  @Override
  public void removeHeader(String name) {
    headers.remove(name);
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Method getMethod() {
    return method;
  }

  @Override
  public Optional<String> getContentType() {
    return headers.getFirstValue("Content-Type");
  }

  @Override
  public Optional<Object> getBody() {
    return Optional.ofNullable(body);
  }

  @Override
  public boolean isFormEncoded() {
    return getMethod() == Method.POST
        && getContentType()
            .filter(ct -> ct.equals("application/x-www-form-urlencoded"))
            .isPresent();
  }

  @Override
  public void addResponseCallback(BiConsumer<ResponseContext, Exception> responseCallback) {
    if (responseCallbacks == null) {
      responseCallbacks = new ArrayList<>();
    }
    responseCallbacks.add(responseCallback);
  }

  @Override
  public List<BiConsumer<ResponseContext, Exception>> getResponseCallbacks() {
    return responseCallbacks;
  }

  @Override
  public String toString() {
    return method.toString() + " " + uri;
  }
}
