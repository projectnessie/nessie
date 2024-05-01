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

import static org.projectnessie.client.NessieConfigConstants.CONF_ENABLE_API_COMPATIBILITY_CHECK;

import java.net.URI;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;

/**
 * This is the <em>deprecated</em> builder class to create a {@link NessieApi} instance for
 * HTTP/REST.
 *
 * <p>Note that this class does not build an HTTP client but a Nessie API client.
 *
 * @deprecated This class is deprecated for removal. Migrate your code to use {@link
 *     NessieClientBuilder#createClientBuilder(String, String)}
 */
@Deprecated
public class HttpClientBuilder extends NessieHttpClientBuilderImpl {

  @Deprecated // for removal
  public static final String ENABLE_API_COMPATIBILITY_CHECK_SYSTEM_PROPERTY =
      CONF_ENABLE_API_COMPATIBILITY_CHECK;

  public HttpClientBuilder() {}

  /**
   * Migrate calling code to use {@link
   * NessieClientBuilder#createClientBuilderFromSystemSettings()}.
   */
  @Deprecated
  public static HttpClientBuilder builder() {
    return new HttpClientBuilder();
  }

  @Override
  public HttpClientBuilder withUri(String uri) {
    return (HttpClientBuilder) super.withUri(uri);
  }

  @SuppressWarnings("deprecation")
  @Override
  public HttpClientBuilder fromSystemProperties() {
    return (HttpClientBuilder) super.fromSystemProperties();
  }

  @Override
  public HttpClientBuilder fromConfig(Function<String, String> configuration) {
    return (HttpClientBuilder) super.fromConfig(configuration);
  }

  @Override
  public HttpClientBuilder withAuthenticationFromConfig(Function<String, String> configuration) {
    return (HttpClientBuilder) super.withAuthenticationFromConfig(configuration);
  }

  @Override
  public HttpClientBuilder withUri(URI uri) {
    return (HttpClientBuilder) super.withUri(uri);
  }

  @Override
  public HttpClientBuilder withAuthentication(NessieAuthentication authentication) {
    return (HttpClientBuilder) super.withAuthentication(authentication);
  }

  @Override
  public HttpClientBuilder withTracing(boolean tracing) {
    return (HttpClientBuilder) super.withTracing(tracing);
  }

  @Override
  public HttpClientBuilder withReadTimeout(int readTimeoutMillis) {
    return (HttpClientBuilder) super.withReadTimeout(readTimeoutMillis);
  }

  @Override
  public HttpClientBuilder withConnectionTimeout(int connectionTimeoutMillis) {
    return (HttpClientBuilder) super.withConnectionTimeout(connectionTimeoutMillis);
  }

  @Override
  public HttpClientBuilder withDisableCompression(boolean disableCompression) {
    return (HttpClientBuilder) super.withDisableCompression(disableCompression);
  }

  @Override
  public HttpClientBuilder withSSLContext(SSLContext sslContext) {
    return (HttpClientBuilder) super.withSSLContext(sslContext);
  }

  @Override
  public HttpClientBuilder withSSLParameters(SSLParameters sslParameters) {
    return (HttpClientBuilder) super.withSSLParameters(sslParameters);
  }

  @Override
  public HttpClientBuilder withHttp2Upgrade(boolean http2Upgrade) {
    return (HttpClientBuilder) super.withHttp2Upgrade(http2Upgrade);
  }

  @Override
  public HttpClientBuilder withFollowRedirects(String redirects) {
    return (HttpClientBuilder) super.withFollowRedirects(redirects);
  }

  @Override
  @Deprecated
  @SuppressWarnings("deprecation")
  public HttpClientBuilder withForceUrlConnectionClient(boolean forceUrlConnectionClient) {
    return (HttpClientBuilder) super.withForceUrlConnectionClient(forceUrlConnectionClient);
  }

  @Override
  public HttpClientBuilder withClientName(String clientName) {
    return (HttpClientBuilder) super.withClientName(clientName);
  }

  @Override
  public HttpClientBuilder withApiCompatibilityCheck(boolean enable) {
    return (HttpClientBuilder) super.withApiCompatibilityCheck(enable);
  }

  @Override
  public HttpClientBuilder withResponseFactory(HttpResponseFactory responseFactory) {
    return (HttpClientBuilder) super.withResponseFactory(responseFactory);
  }

  @Override
  public HttpClientBuilder addRequestFilter(RequestFilter filter) {
    return (HttpClientBuilder) super.addRequestFilter(filter);
  }

  @Override
  public HttpClientBuilder addResponseFilter(ResponseFilter filter) {
    return (HttpClientBuilder) super.addResponseFilter(filter);
  }

  @Override
  public <API extends NessieApi> API build(Class<API> apiVersion) {
    return super.build(apiVersion);
  }
}
