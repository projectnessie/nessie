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
package org.projectnessie.client.http.impl.apache;

import java.io.IOException;
import java.net.URI;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

final class ApacheHttpClient implements HttpClient {
  final HttpRuntimeConfig config;
  final CloseableHttpClient client;

  ApacheHttpClient(HttpRuntimeConfig config) {
    this.config = config;

    PoolingHttpClientConnectionManagerBuilder connManager =
        PoolingHttpClientConnectionManagerBuilder.create()
            .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
            .setConnPoolPolicy(PoolReusePolicy.LIFO);

    if (config.getSslContext() != null) {
      connManager.setTlsSocketStrategy(new DefaultClientTlsStrategy(config.getSslContext()));
    }

    connManager.setDefaultSocketConfig(
        SocketConfig.custom()
            .setTcpNoDelay(true)
            .setSoTimeout(Timeout.ofMilliseconds(config.getReadTimeoutMillis()))
            .setTcpNoDelay(true)
            .build());

    connManager.setDefaultConnectionConfig(
        ConnectionConfig.custom()
            .setTimeToLive(TimeValue.ofMinutes(5))
            .setValidateAfterInactivity(TimeValue.ofSeconds(10))
            .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeoutMillis()))
            .build());

    connManager.setMaxConnTotal(100);
    connManager.setMaxConnPerRoute(10);

    RequestConfig defaultRequestConfig =
        RequestConfig.custom()
            .setResponseTimeout(Timeout.ofMilliseconds(config.getReadTimeoutMillis()))
            .setRedirectsEnabled(true)
            .setCircularRedirectsAllowed(false)
            .setMaxRedirects(5)
            .setContentCompressionEnabled(!config.isDisableCompression())
            .build();

    HttpClientBuilder clientBuilder =
        HttpClients.custom()
            .disableDefaultUserAgent()
            .disableAuthCaching()
            .disableCookieManagement()
            .setConnectionManager(connManager.build())
            .setDefaultRequestConfig(defaultRequestConfig);
    if (config.isDisableCompression()) {
      clientBuilder.disableContentCompression();
    }

    client = clientBuilder.build();
  }

  @Override
  public HttpRequest newRequest(URI baseUri) {
    return new ApacheRequest(this, baseUri);
  }

  @Override
  public URI getBaseUri() {
    return config.getBaseUri();
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      config.close();
    }
  }
}
