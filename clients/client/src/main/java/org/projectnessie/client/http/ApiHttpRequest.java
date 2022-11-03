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
package org.projectnessie.client.http;

import java.util.function.Supplier;

/**
 * This is a helper class for use with an {@link HttpRequest} that will unwrap the specified
 * API-level exceptions from {@link HttpClientException} thrown during the execution of the {@link
 * HttpRequest}.
 *
 * <p>Currently this class supports up to two distinct API-level exception types, but it can easily
 * be extended to support more if required.
 *
 * <p>The exception types to be unwrapped are specifies as arguments to {@link
 * HttpRequest#unwrap(Class)} and {@link HttpRequest#unwrap(Class, Class)} calls.
 *
 * @param <E1> the first API-level exception that should be unwrapped
 * @param <E2> the second API-level exception that should be unwrapped
 */
public class ApiHttpRequest<E1 extends Throwable, E2 extends Throwable> {
  private final HttpRequest request;
  private final Class<E1> ex1;
  private final Class<E2> ex2;

  ApiHttpRequest(HttpRequest request, Class<E1> ex1, Class<E2> ex2) {
    this.request = request;
    this.ex1 = ex1;
    this.ex2 = ex2;
  }

  public HttpResponse get() throws E1, E2 {
    return unwrap(request::get);
  }

  public HttpResponse delete() throws E1, E2 {
    return unwrap(request::delete);
  }

  public HttpResponse post(Object obj) throws E1, E2 {
    return unwrap(() -> request.post(obj));
  }

  public HttpResponse put(Object obj) throws E1, E2 {
    return unwrap(() -> request.put(obj));
  }

  private HttpResponse unwrap(Supplier<HttpResponse> action) throws E1, E2 {
    try {
      return action.get();
    } catch (HttpClientException e) {
      Throwable cause = e.getCause();

      if (ex1.isInstance(cause)) {
        throw ex1.cast(cause);
      }

      if (ex2.isInstance(cause)) {
        throw ex2.cast(cause);
      }

      throw e;
    }
  }
}
