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
package org.projectnessie.client.http.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.immutables.value.Value;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpResponseFactory;
import org.projectnessie.client.http.RequestFilter;
import org.projectnessie.client.http.ResponseFilter;

/** Implementation specific HTTP configuration holder. */
@Value.Immutable
public interface HttpRuntimeConfig extends AutoCloseable {

  static ImmutableHttpRuntimeConfig.Builder builder() {
    return ImmutableHttpRuntimeConfig.builder();
  }

  @Value.Check
  default void check() {
    URI baseUri = getBaseUri();
    if (baseUri != null && !HttpUtils.isHttpUri(baseUri)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot start http client. %s must be a valid http or https address", baseUri));
    }
  }

  @Nullable
  @jakarta.annotation.Nullable
  URI getBaseUri();

  ObjectMapper getMapper();

  @Nullable
  @jakarta.annotation.Nullable
  Class<?> getJsonView();

  HttpResponseFactory responseFactory();

  int getReadTimeoutMillis();

  int getConnectionTimeoutMillis();

  boolean isDisableCompression();

  SSLContext getSslContext();

  @Nullable
  HttpAuthentication getAuthentication();

  List<RequestFilter> getRequestFilters();

  List<ResponseFilter> getResponseFilters();

  @Nullable
  @jakarta.annotation.Nullable
  String getFollowRedirects();

  @Nullable
  @jakarta.annotation.Nullable
  SSLParameters getSslParameters();

  @Value.Default
  default boolean isHttp11Only() {
    // TODO Jersey/Grizzly has a serious bug that prevents it from working with Java's new HTTP
    //  client, if the HTTP client's not tied to a particular HTTP version.
    //  Background: HTTP clients send an 'Upgrade' header using HTTP/1.1 in the first request.
    //  This lets Jersey/Grizzly call
    //  'org.glassfish.grizzly.http.HttpHeader.setIgnoreContentModifiers(true)'
    //  from 'org.glassfish.grizzly.http.HttpCodecFilter.handleRead', which __prevents__ that the
    //  response contains the 'Content-Type' header, which in turn lets Nessie's HTTP client fail,
    //  because there's not 'Content-Type' header.
    //  Quarkus is not affected by the above issue.

    return true;
  }

  @Value.Default
  default int getClientSpec() {
    return 2;
  }

  @Override
  default void close() {
    HttpAuthentication authentication = getAuthentication();
    if (authentication != null) {
      authentication.close();
    }
  }
}
