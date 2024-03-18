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
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.config.RegistryBuilder;
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

    RegistryBuilder<ConnectionSocketFactory> socketFactoryRegistryBuilder =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("http", PlainConnectionSocketFactory.INSTANCE);

    if (config.getSslContext() != null) {
      SSLConnectionSocketFactory sslConnectionSocketFactory =
          new SSLConnectionSocketFactory(config.getSslContext());
      socketFactoryRegistryBuilder.register("https", sslConnectionSocketFactory);
    }

    PoolingHttpClientConnectionManager connManager =
        new PoolingHttpClientConnectionManager(
            socketFactoryRegistryBuilder.build(),
            PoolConcurrencyPolicy.STRICT,
            PoolReusePolicy.LIFO,
            TimeValue.ofMinutes(5));

    SocketConfig socketConfig =
        SocketConfig.custom()
            .setTcpNoDelay(true)
            .setSoTimeout(Timeout.ofMilliseconds(config.getReadTimeoutMillis()))
            .setTcpNoDelay(true)
            .build();

    connManager.setDefaultSocketConfig(socketConfig);
    connManager.setValidateAfterInactivity(TimeValue.ofSeconds(10));
    connManager.setMaxTotal(100);
    connManager.setDefaultMaxPerRoute(10);

    RequestConfig defaultRequestConfig =
        RequestConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeoutMillis()))
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
            .setConnectionManager(connManager)
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
