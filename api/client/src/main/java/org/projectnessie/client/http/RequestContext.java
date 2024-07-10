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
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.projectnessie.client.http.HttpClient.Method;

/** Context containing all important info about a request. */
public interface RequestContext {

  void putHeader(String name, String value);

  boolean containsHeader(String name);

  void removeHeader(String name);

  URI getUri();

  Method getMethod();

  Optional<String> getContentType();

  boolean isFormEncoded();

  Optional<Object> getBody();

  default boolean doesOutput() {
    return (getMethod() == Method.PUT || getMethod() == Method.POST) && getBody().isPresent();
  }

  /**
   * Adds a callback to be called when the request has finished. The {@code responseCallback} {@link
   * BiConsumer consumer} is called with a non-{@code null} {@link ResponseContext}, if the HTTP
   * request technically succeeded. The {@code responseCallback} {@link BiConsumer consumer} is
   * called with a non-{@code null} {@link Exception} object, if the HTTP request technically
   * failed.
   *
   * @param responseCallback callback that receives either a non-{@code null} {@link
   *     ResponseContext} or a non-{@code null} {@link Exception}.
   */
  void addResponseCallback(BiConsumer<ResponseContext, Exception> responseCallback);

  List<BiConsumer<ResponseContext, Exception>> getResponseCallbacks();
}
